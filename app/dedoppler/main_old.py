"""
1. scan for new files in data
2. path, ext, size, first seen, md5sum
3. scan /import/new
 - find + rename internal duplicates, move files
 - compare lower(ext) and filesize

Load mode from config.json
1: move all files to correct folders
2: only log duplicate files to duplicates.txt
"""

import os
import hashlib
import csv
import time
import collections
import shutil
import pandas as pd
from pathlib import Path, PurePath

#variables
data_folder = '/app/data/'
import_folder = '/app/import/'
duplicate_folder = '/app/import/duplicates/'
added_folder = '/app/import/added/'
removed_folder = '/app/import/removed/'
history_csv = '/app/db/history.csv'
current_csv = '/app/db/current.csv'
temp_csv = '/app/db/temp.csv'
csv_headers = ['file_path','file_name','ext','size','md5sum','first_seen']

def create_csv():
    csv_list = [history_csv, current_csv]
    for file in csv_list:
        if Path(file).is_file():
            pass
        else:
            with open(file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=csv_headers)
                writer.writeheader()

def create_folders():
    folder_list = [duplicate_folder, added_folder, removed_folder]
    for folder in folder_list:
        if not os.path.exists(folder):
            os.makedirs(folder)

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def add_dict_to_csv(csv_file, dictionary):
    with open(csv_file,'a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=csv_headers)
        writer.writerows([dictionary])

def scan_import_for_duplicates():
    """
    Scan import_folder for duplicate files, move them to 'duplicates'
    :return:
    """
    top_level_file_list = [i for i in os.listdir(import_folder) if os.path.isfile(os.path.join(import_folder, i))]
    process_dict_list = []
    ext_list = []
    size_list = []
    for file in top_level_file_list:
        filepath = PurePath(import_folder, file)
        ext = str(filepath).rsplit('.', 1)[-1].lower()
        ext_list.append(ext)
        size = os.stat(filepath).st_size
        size_list.append(size)
        file_dict = {'filepath': filepath,
                     'file': file,
                     'ext': ext,
                     'size': size,
                     'md5sum': ''}
        process_dict_list.append(file_dict)
    #print(process_dict_list)
    if len(top_level_file_list) == len(set(size_list)):
        # all files in import_folder have different sizes -> contain no duplicates
        print(f'  No duplicates were detected in {import_folder}')
    else:
        duplicate_sizes = [item for item, count in collections.Counter(size_list).items() if count > 1]
        #print(duplicate_sizes)
        md5_dup_list = []
        for duplicate_size in duplicate_sizes:
            for file_dict in process_dict_list:
                if file_dict['size'] == duplicate_size:
                    filepath = file_dict['filepath']
                    md5sum = md5(filepath)
                    md5_dup_list.append(md5sum)
                    file_dict.update({'md5sum': md5sum})
        #print(process_dict_list)
        duplicate_md5_list = [item for item, count in collections.Counter(md5_dup_list).items() if count > 1]
        #print(duplicate_md5_list)
        a = 0
        if len(duplicate_md5_list) == 0:
            print(f'  No duplicates were detected in {import_folder}')
        elif len(duplicate_md5_list) == 1:
            for index, duplicate_md5 in enumerate(duplicate_md5_list):
                for file_dict in process_dict_list:
                    if file_dict['md5sum'] == duplicate_md5:
                        filepath = file_dict['filepath']
                        file = file_dict['file']
                        #print(f'Moving {filepath} to duplicates')
                        a += 1
                        shutil.move(filepath, duplicate_folder + file)
        else:
            for index, duplicate_md5 in enumerate(duplicate_md5_list):
                for file_dict in process_dict_list:
                    if file_dict['md5sum'] == duplicate_md5:
                        filepath = file_dict['filepath']
                        file = file_dict['file']
                        #print(f'Moving {filepath} to duplicates')
                        a += 1
                        shutil.move(filepath, duplicate_folder + str(index) + '_' + file)
        print(f'  {a} duplicates (from {len(duplicate_md5_list)} unique files) were detected')


def scan_import():
    """
    Scan import_folder for new files, move them accordingly
    :return:
    """
    top_level_folder_list = next(os.walk(import_folder))[1]
    top_level_file_list = [i for i in os.listdir(import_folder) if os.path.isfile(os.path.join(import_folder, i))]
    if len(top_level_file_list) == 0:
        print(f'  No files to process found in {import_folder}')
    for index, file in enumerate(top_level_file_list):
        print(f'  Processing file {index + 1}/{len(top_level_file_list)}: {file}')
        filepath = PurePath(import_folder, file)
        ext = str(filepath).rsplit('.', 1)[-1].lower()
        size = os.stat(filepath).st_size
        # check first if CSV contains a file with same ext and size before comparing md5
        df_combined = pd.concat([pd.read_csv(current_csv), pd.read_csv(history_csv)])
        df_1 = df_combined[(df_combined['ext'] == ext) & (df_combined['size'] == size)]
        if df_1.shape[0] == 0:
            shutil.move(filepath, added_folder + file)
            print('    File added after size check.')
        else:
            md5sum = md5(filepath)
            history_md5_list = df_1['md5sum'].tolist()
            if md5sum in history_md5_list:
                shutil.move(filepath, removed_folder + file)
                print('    File removed after md5 check.')
            else:
                shutil.move(filepath, added_folder + file)
                print('    File added after md5 check.')

def scan_data():
    """
    Scan data_folder for all files, add new files to csv
    :return:
    """
    file_list = []
    file_list_b = []
    for path, subdirs, files in os.walk(data_folder):
        for name in files:
            purepath = PurePath(path, name)
            file_list.append([str(purepath), str(name)])
            file_list_b.append(str(purepath))

    # add new files to CSV
    a = 0
    b = 0
    for index, file in enumerate(file_list):
        file_path = file[0]
        file_name = file[1]
        ext = file_name.rsplit('.',1)[-1].lower()
        size = os.stat(file_path).st_size  # filesize in bytes
        scraping_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        # check first if CSV contains a file with same ext and size before calculating md5
        df_combined = pd.concat([pd.read_csv(current_csv), pd.read_csv(history_csv)])
        df_1 = df_combined[(df_combined['file_path'] == file_path) & (df_combined['size'] == size)]
        if df_1.shape[0] == 0:
            # add file to CSV
            md5sum = md5(file_path)
            file_dict = {
                'file_path': file_path,
                'file_name': file_name,
                'ext': ext,
                'size': size,
                'md5sum': md5sum,
                'first_seen': scraping_time
            }
            # print(f'{index + 1}: {file_name} - {md5sum} - {size} - {scraping_time}')
            add_dict_to_csv(current_csv, file_dict)
            #print(f'Added file {file_name} to CSV')
            a += 1
        else:
            #print(f'File {file_name} is already in CSV')
            b += 1
            continue
    print(f'  Added {a} new files to CSV, {b} were already present.')

    # mark removed files in CSV
    df_current = pd.read_csv(current_csv)
    present_files_path_list = df_current['file_path'].tolist()
    removed_files = [x for x in present_files_path_list if x not in file_list_b]
    if len(removed_files) != 0:
        print(f'  {len(removed_files)} removed file(s) detected...')
        # 1. add to history.csv
        for file in removed_files:
            with open(current_csv, 'r') as data:
                for line in csv.DictReader(data):
                    if line['file_path'] == file:
                        add_dict_to_csv(history_csv, line)
        # 2. remove from current.csv
        with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers)
            writer.writeheader()
        with open(current_csv, 'r') as r:
            for line in csv.DictReader(r):
                if line['file_path'] not in removed_files:
                    add_dict_to_csv(temp_csv, line)
        shutil.move(temp_csv, current_csv)

create_csv()
create_folders()
print(f'Step 1: Updating the current.csv and history.csv based on {data_folder}')
scan_data()
print(f'Step 2: Searching for duplicate files within {import_folder}')
scan_import_for_duplicates()
print(f'Step 3: Searching for duplicates between {import_folder} and {data_folder}')
scan_import()