# Migration Blobs

A simple tool for migration of large objects from old PostgreSQL.

### Usage

```shell script
$ git clone git@github.com:LevZaplatin/migration-blobs.git
$ cd migration-blobs/
$ virtualenv .venv
$ source .venv/bin/activate
(.venv) $ pip install -r requirements.txt
(.venv) $ python3 migration-blobs.py backup -H 127.0.0.1 -w -U pgsql -p 5432 -d app -o ./blobs/ -c 5000
(.venv) $ python3 migration-blobs.py restore -H 127.0.0.1 -w -U pgsql -p 5432 -d app -o ./blobs/
```

### Output file

```sql
SELECT pg_catalog.lo_create('1201210');
SELECT pg_catalog.lo_open('1201210', 131072);
SELECT pg_catalog.lowrite(0, '\x<hex content>');
SELECT pg_catalog.lowrite(0, '\x<hex content>');
SELECT pg_catalog.lowrite(0, '\x<hex content>');
SELECT pg_catalog.lowrite(0, '\x<hex content>');
SELECT pg_catalog.lowrite(0, '\x<hex content>');
SELECT pg_catalog.lo_close(0);
``` 

### Help

```text
usage: migration-blobs.py [-h] [-W PASSWORD] [-w] [-p PORT] [-v] -H HOST -d
                          DATABASE -U USER -o PATH
                          action

Migration tool for generating SQL dump and restoring it

positional arguments:
  action                backup or restore

optional arguments:
  -h, --help            show this help message and exit
  -W PASSWORD, --password PASSWORD
                        PG password
  -w, --no-password     Empty password
  -p PORT, --port PORT  PG port (default: 5432)
  -v, --verbose         Verbose mode

required arguments:
  -H HOST, --host HOST  PG hostname
  -d DATABASE, --database DATABASE
                        PG database
  -U USER, --user USER  PG user
  -o PATH, --path PATH  Storage folder
```