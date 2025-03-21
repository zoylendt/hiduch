import move
import scan
import db

# 1. create folders
move.create_folders()
# 1b. search for CSV, import to DB
...
# 2. search in 'data' for new & removed files, update DB
file_dict_list = scan.quick_scan('/workspaces/hiduch/app/test files/data')
list_of_paths_in_db = db.get_all_paths() # ToDo: re-add string before each path
for file in file_dict_list:
    if file['path'] not in list_of_paths_in_db:
        file['md5sum'] = scan.md5(file['path'])
        db.add_new_file(file)
missing_files = [x for x in list_of_paths_in_db if x not in file_dict_list]
for file in missing_files:
    file_id = db.get_id_of_file(file)
    db.mark_file_as_deleted(file_id)
# 3. search in 'import' for duplicates
...
# 4. search for duplicates between 'import' and 'data'
...
# 5. move files, restore file ownership
...
# 6. update DB (step #2 again)
...
# 7. signal process end (create/delete signal file in 'import'?)
...