import os
import re
from datetime import datetime
import pandas as pd
from multiprocessing import Pool

# keywords
keywords = ["trump", "maga", "MakeAmericaGreatAgain", "Make America Great Again", "sleepy joe", 
            "sleepyjoe", "AmericaFirst", "kag", "pence", "VoteRed2020", 
            "biden", "joe2020", "teamjoe", "kamala", "harris", "demconvention", "demdebate", 
            "BattleForTheSoulOfTheNation", "Battle For The Soul Of The Nation", "VoteBlue2020", 
            "election2020", "elections2020", "debates2020"]

# file path
source_path = "/mnt/data/Project7/fakenews/csv/"
target_path = "data/csv_filtered/"

# set file range
start_num = 13
end_num = 17

# settings for result filename
label = 'en+POTUS2020'
content = 'tweet'

# format of source filename
file_num_pattern = r'(\d+)\.csv'

# other settings
chunksize = 1e6
proc_num = 10

def process_file(source_name):
    # check file name
    mtch = re.search(file_num_pattern, source_name)
    if mtch:
        # get file number from file name
        file_num = int(mtch.group(1))
        
        # check if the date in time range. if yes, process the file.
        if start_num <= file_num <= end_num:
            print(f"processing {source_name} ... ...\n", end='')
            target_name = f"{content}[{label}][{file_num}].csv"

            # check if the target file already exists
            if os.path.exists(target_path + target_name):
                print(f"{target_name} already exists.\n", end='')
                return

            try:
                for chunk in pd.read_csv(source_path + source_name, chunksize=chunksize, low_memory=False):
                    df = chunk[
                            chunk['text'].str.contains('|'.join(keywords), case=False) |
                            chunk['entities.hashtags'].astype(str).str.contains('|'.join(keywords), case=False)
                        ]
                    df.to_csv(target_path + target_name, mode='a', index=False)
                print(f"{source_name} processed.\n", end='')
            except Exception as e:
                print(f"failed processing {source_name}.\n{e}\n", end='')

# get a list of files
files = os.listdir(source_path)

# create a pool of workers
with Pool(processes=proc_num) as pool:
    pool.map(process_file, files)

print("Done.")