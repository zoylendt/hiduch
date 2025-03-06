from sqlmodel import SQLModel, Field, create_engine, Session, Relationship, select
import time
import html
import pprint

engine = create_engine('sqlite:///szurule34.db')

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

# ---

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

