# move files
# create directories
# handle file ownership

import os

data_folder = '/workspaces/hiduch/app/test files/data'
duplicate_folder = '/workspaces/hiduch/app/test files/import/duplicates'
accepted_folder = '/workspaces/hiduch/app/test files/import/accepted'
rejected_folder = '/workspaces/hiduch/app/test files/import/rejected'

def create_folders():
    folder_list = [duplicate_folder, accepted_folder, rejected_folder]
    for folder in folder_list:
        if not os.path.exists(folder):
            os.makedirs(folder)