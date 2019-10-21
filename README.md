# Migration Blobs

A simple tool for migration of large objects from old PostgreSQL.

### Usage

```shell script
python3 backup.py -H 127.0.0.1 -w -U pgsql -p 5432 -d app -o ./blobs/ -c 5000
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
usage: backup.py [-h] [-W PASSWORD] [-w] [-p PORT] [-c CHUNK] -H HOST -d
                 DATABASE -U USER -o PATH

Make backup (SQL) blobs

optional arguments:
  -h, --help            show this help message and exit
  -W PASSWORD, --password PASSWORD
                        PG password
  -w, --no-password     Empty password
  -p PORT, --port PORT  PG port (default: 5432)
  -c CHUNK, --chunk CHUNK
                        Chunk size (default: 1000)

required arguments:
  -H HOST, --host HOST  PG hostname
  -d DATABASE, --database DATABASE
                        PG database
  -U USER, --user USER  PG user
  -o PATH, --path PATH  Storage folder
```