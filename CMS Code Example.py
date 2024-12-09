import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil import parser
import json
from collections import Counter

# Set metadata file location, output directory, and dataset URL
METADATA_FILE = "last_run_metadata.json"
OUTPUT_DIR = "hospital_data"
DATASETS_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
NUM_WORKERS = 4  # Parallel downloads

# Load metadata file with last execution date
def load_metadata():
    """Load last run metadata file."""
    try:
        if os.path.exists(METADATA_FILE):
            with open(METADATA_FILE, "r") as file:
                return json.load(file)
    except Exception as e:
        print(f"Error loading metadata file: {e}")
    return {"last_run": "1970-01-01T00:00:00Z"}

# Bookmark metadata
def save_metadata(metadata):
    """Save updated metadata."""
    try:
        with open(METADATA_FILE, "w") as file:
            json.dump(metadata, file)
    except Exception as e:
        print(f"Error saving metadata: {e}")

# Convert column names to snake case
def convert_to_snake_case(column_name):
    """Convert column names to snake_case."""
    return (
        column_name.strip()
        .lower()
        .replace(" ", "_")
        .replace("'", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
        .replace("/", "_")
        .replace(":", "_")
    )

# Retrieve datasets from JSON URL downloadUrl values
def fetch_datasets():
    """Fetch datasets from the CMS API."""
    try:
        response = requests.get(DATASETS_URL)
        if response.status_code == 200:
            print("Successfully fetched datasets.")
            return response.json()  # Return the list directly
        else:
            print(f"Failed to fetch datasets: HTTP {response.status_code}")
    except Exception as e:
        print(f"Error fetching datasets: {e}")
    return []

# Process CSV files to hospital_data folder
def download_and_process_csv(download_url, file_name):
    """Download and process a CSV file."""
    output_path = os.path.join(OUTPUT_DIR, file_name)
    try:
        response = requests.get(download_url)
        if response.status_code == 200:
            with open(output_path, "wb") as file:
                file.write(response.content)
            print(f"Downloaded: {file_name}")

            # Process the CSV
            df = pd.read_csv(output_path)
            df.columns = [convert_to_snake_case(col) for col in df.columns]
            df.to_csv(output_path, index=False)
            print(f"Processed: {file_name}")
        else:
            print(f"Failed to download {file_name}: HTTP {response.status_code}")
    except Exception as e:
        print(f"Error processing {file_name}: {e}")

def main():
    """Main function to orchestrate processing."""
    datasets = fetch_datasets()
    if not datasets:
        print("No datasets available.")
        return

    # Load metadata
    metadata = load_metadata()
    last_run_time = parser.isoparse(metadata["last_run"])
    print(f"Last run time: {last_run_time}")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Collect themes and filtered datasets
    theme_counts = Counter()
    filtered_datasets = []
    for ds in datasets:
        themes = ds.get("theme", [])
        theme_counts.update(themes)

        # Check if the dataset belongs to hospitals
        if (
            "Hospital" in themes
            or "hospital" in ds.get("title", "").lower()
            or "hospital" in ds.get("description", "").lower()
        ):
            try:
                # Ensure modified date is offset-naive
                modified_date = parser.isoparse(ds.get("modified")).replace(tzinfo=None)
                if modified_date > last_run_time.replace(tzinfo=None):
                    filtered_datasets.append(ds)
            except Exception as e:
                print(f"Error parsing modified date for {ds.get('title')}: {e}")

    # Log filtered datasets
    print(f"\nFiltered datasets ({len(filtered_datasets)} total):")
    for ds in filtered_datasets:
        print(f"- Title: {ds.get('title')}, Modified: {ds.get('modified')}")

    if not filtered_datasets:
        print("No new datasets to download.")
        return

    # Collect URLs and file names
    tasks = []
    for ds in filtered_datasets:
        for dist in ds.get("distribution", []):
            if dist.get("mediaType") == "text/csv":
                download_url = dist.get("downloadURL")
                file_name = download_url.split("/")[-1]
                tasks.append((download_url, file_name))

    # Parallel CSV processing
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        executor.map(lambda task: download_and_process_csv(*task), tasks)

    # Update last run time in metadata file
    metadata["last_run"] = datetime.utcnow().isoformat()
    save_metadata(metadata)

    # Log theme counts
    print("\nProcessing complete!")
    print(f"Total records retrieved: {len(datasets)}")
    print(f"Theme counts:")
    for theme, count in theme_counts.items():
        print(f"- {theme}: {count}")
    print(f"Hospitals processed: {len(filtered_datasets)}")

if __name__ == "__main__":
    main()
