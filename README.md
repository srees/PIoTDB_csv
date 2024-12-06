# PIoTDB_csv
Authors: Stephen Rees, Jonathan Sprinkle
License: BSD Copyright (c) 2024, Vanderbilt University

Repo contains a collection of work used over the course of an independent study on implementing IoTDB.
Starting condition was a series of CSV files forming a dataset that needed to be imported. After import was
complete, I experimented with different queries and how to plot the information from the database.
iotdb_batch_import.py is the primary file to use for data import into IoTDB

Several of the files are related to looking for duplicates in the CSV files, as there was a mismatch on the
number of rows returned vs what was expected. This ended up actually being a difference between counting rows
based on the systime column vs using the experiment date, which is encoded into the file_tag_id. The scripts
checking for duplicates were left for reference.

* circles100.ipynb is a Jupyter notebook used for much of the experimentation after the data was imported. The 
notebook depends on Python 3.12.7 pip was used to install apache-iotdb, geopy, mplcursors, scipy and shapely.
The import scripts shifted a bit over the course of the semester. Initially, the idea was to import can_bus_data
as unaligned data and the GPS_data as aligned data. This ended up complicating queries more than it was worth
and so later the import was redone with all data as unaligned data. The top of the notebook contains the
database structure.

* count_norm_stamps.py counts the number of unique timestamps (first column) in a CSV. Note this does not reflect unique
records, as multiple measurements can land on the same timestamp.

* find_any_dups.py attempts to locate duplicate rows in a CSV that may not be sequential
* find_norm_seq_dups.py attempts to locate sequential duplicate rows based on the first 5 fields while normalizing the systime field
* find_seq_dups.py attempts to locate sequential duplicate rows
* iotdb_batch_import.py was an attempt to unify the ability to import aligned and unaligned data into IoTDB into a single script
* link_mm_locations.csv is the CSV containing mile marker locations on I-24 used for interpolating data in the circles100 notebook
* unaligned_iotdb_batch_import.py is a rewrite of v1_iotdb_import designed to handle batch importing of data for efficiency
* v1_iotdb_import.py was my first working example of getting data into IoTDB from a CSV, one row at a time (slow)
* verify.py was an initial cut at being able to count/verify the number of records for a given time-series in IoTDB. This functionality
  was included in the final iotdb_batch_import.py file.

  Database credentials in this repo are set to the defaults for IoTDB - update as needed.
  On-site or VPN required for accessing CIRCLES data.
