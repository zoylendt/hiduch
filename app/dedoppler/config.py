import os

fileSizeInBytes = 1524
with open('file_e.jpg', 'wb') as fout:
    fout.write(os.urandom(fileSizeInBytes))