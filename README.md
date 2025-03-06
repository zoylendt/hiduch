HiDuCh - History Duplicate Checker

---

Check if files from the 'import' folder have previously been in the 'data' folder.  
Use case: prevent a file from being added to a folder if it has been in that folder before.

Folders:
  - /config (currently not used)
  - /import
    - added
    - duplicates
    - removed
  - /db (CSV + logs)
  - /data (mass storage folder, read-only possible)

ToDo:
  - add logging to file
  - use config (for different modes)
  - add path length check
  - add mode to rename files with non-utf-7 characters
  - add detailed setup guide
  - add GH action to automatically build and push to GHCR

---

Folders:
/data
/scan
  /accepted
  /rejected