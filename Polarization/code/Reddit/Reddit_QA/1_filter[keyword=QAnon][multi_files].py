import os
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

keywords = ['QAnon']

# Define the directory to traverse
target_path = "data/csv[keyword=QAnon]/"
source_path = "/mnt/data/reddit/reddit/RC/"

# Define the date range
start_date = datetime.strptime("2020-04", "%Y-%m")
end_date = datetime.strptime("2021-04", "%Y-%m")

# Define the function to process a file
def process_file(filename):
    # Check if the file has already been processed
    if os.path.exists(target_path + f"{filename[3:10]}.csv"):
        print(f"{filename} has already been processed.")
        return

    print(f"Processing {filename}...")
    df = pd.read_csv(source_path + filename)
    df = df[df['body'].astype(str).str.contains('|'.join(keywords), case=False)]
    df.to_csv(target_path + f"{filename[3:10]}.csv", index=False)
    print(f"{filename} processed.")

# Get the list of files to process
files_to_process = []
for filename in os.listdir(source_path):
    # Check if the filename matches the pattern "RC_YYYY-MM.zst.csv"
    if filename.startswith("RC_") and filename.endswith(".zst.csv"):
        # Extract the date from the filename
        date_str = filename[3:10]
        file_date = datetime.strptime(date_str, "%Y-%m")
        
        # Check if the file date is within the specified range
        if start_date <= file_date <= end_date:
            files_to_process.append(filename)

# Use a ThreadPoolExecutor to process the files in parallel
with ThreadPoolExecutor(max_workers=2) as executor:
    executor.map(process_file, files_to_process)