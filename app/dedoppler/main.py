import move
import scan
import db

from sqlmodel import SQLModel, Field, create_engine, Session, Relationship, select
import os
import hashlib
import pathlib
import collections
import time
import html
import pprint

# --- Variables ---
# ToDo: read from config file

db_path = '/app/config/dedoppler.db'
data_folder = '/app/data/'
import_folder = '/app/import/'
duplicate_folder = '/app/import/duplicates/'
accepted_folder = '/app/import/accepted/'
rejected_folder = '/app/import/rejected/'

# --- DB stuff ---

sqlite_url = f'sqlite:///{db_path}'
engine = create_engine(sqlite_url, echo=False)

class Files(SQLModel, table=true):
    id: int = Field(default=None, primary_key=True)
    name: str
    ext: str
    path: str
    size: int
    md5sum: str
    first_seen: int
    deleted: bool = Field(default=False)

SQLModel.metadata.create_all(engine)

def add_new_file(file_dict):
    with Session(engine) as session:
        file = Files(
            name= file_dict['name'],
            ext= file_dict['ext'],
            path= file_dict['path'], # ToDo: remove beginning of path
            size= file_dict['size'],
            md5sum= file_dict['md5sum'],
            first_seen= file_dict['first_seen'],
            deleted= file_dict['deleted']
        )
        session.add(file)
        session.commit()

def mark_file_as_deleted(file_id):
    with Session(engine) as session:
        statement = select(Files).where(Files.id == file_id)
        results = session.exec(statement)
        file_obj = results.one() # ToDo: fail if more than one is found
        file_obj.deleted = True
        session.add(file_obj)
        session.commit()
        session.refresh(file_obj)

def get_files_by_size(size):
    with Session(engine) as session:
        statement = select(Files).where(Files.size == size and Files.deleted == False)
        results = session.exec(statement).all()
        return results

def get_all_paths():
    with Session(engine) as session:
        statement = select(Files).where(Files.deleted == False)
        results = session.exec(statement).all()
        return [x.path for x in results]

def get_id_of_file(path):
    with Session(engine) as session:
        statement = select(Files).where(Files.deleted == False and Files.path == path)
        results = session.exec(statement).one()
        return results.id

# --- Tools ---

def md5(fname):
    # generate md5 hash of a file
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def create_folders():
    # ToDo: set correct permissions
    folder_list = [import_folder, duplicate_folder, accepted_folder, rejected_folder]
    for folder in folder_list:
        if not os.path.exists(folder):
            os.makedirs(folder)

def modify_path(input_str):
    # add/remove path prefix
    prefix = '...'   # path of 'data' in container
    prefix2 = '...'  # for importing from CSV
    if input_str.startswith(prefix):
        return input_str.removeprefix(prefix)
    elif input_str.startswith(prefix2):
        return input_str.removeprefix(prefix2)
    else:
        return str(prefix) + str(input_str)

def move_files(str_src, str_dest):
    # move a file, rename if target exists already
    # inspired by https://stackoverflow.com/a/63323493
    for f in os.listdir(str_src):
        if os.path.isfile(os.path.join(str_src, f)):
            # if not .html continue..
            if not f.endswith(".html"):
                continue

            # count file in the dest folder with same name..
            count = sum(1 for dst_f in os.listdir(str_dest) if dst_f == f)
            
            # prefix file count if duplicate file exists in dest folder
            if count:
                dst_file = f + "_" + str(count + 1)
            else:
                dst_file = f

            shutil.move(os.path.join(str_src, f),
                        os.path.join(str_dest, dst_file))

def move_files(src_file, dst_folder):
    # move a file, rename if target exists already
    # inspired by https://stackoverflow.com/a/63323493
    for f in os.listdir(str_src):
        if os.path.isfile(os.path.join(str_src, f)):
            # if not .html continue..
            if not f.endswith(".html"):
                continue

            # count file in the dest folder with same name..
            count = sum(1 for dst_f in os.listdir(str_dest) if dst_f == f)
            
            # prefix file count if duplicate file exists in dest folder
            if count:
                dst_file = f + "_" + str(count + 1)
            else:
                dst_file = f

            shutil.move(os.path.join(str_src, f),
                        os.path.join(str_dest, dst_file))

# --- Scan ---

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
        filepath = pathlib.PurePath(import_folder, file)
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

# --- CSV import ---

...

# --- Main function ---

# 0. put main function into loop
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