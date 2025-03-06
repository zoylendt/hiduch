import hashlib
import os
import time
import pprint
from pathlib import Path, PurePath

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def quick_scan(folder):
    # scan all files, return list of dictionaries
    return_list = []
    for path, subdirs, files in os.walk(folder):
        for name in files:
            file_dict = {}
            file_dict['name'] = str(name)
            file_dict['path'] = str(PurePath(path, name))
            file_dict['ext'] = str(name).rsplit('.',1)[-1].lower()
            file_dict['first_seen'] = int(time.time())  # current epoch time
            file_dict['size'] = os.stat(PurePath(path, name)).st_size   # filesize in bytes
            return_list.append(file_dict)
    return return_list

#pprint.pprint(quick_scan('/workspaces/hiduch/app/test files/data'))


# --- old:

def scan_data():
    """
    Scan data_folder for all files, add new files to csv
    :return:
    """
    data_folder = x

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