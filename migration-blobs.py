#!/bin/python3

import os
import argparse
import logging
import postgresql

RIGHTS = '131072'
SCHEMA = 'migration'
TABLE_PY_BLOB = 'py_largeobject'
TABLE_PG_BLOB = 'pg_largeobject'
TABLE_PY_RESTORE = 'py_largeobject_restore'

parser = argparse.ArgumentParser(description='Migration tool for generating SQL dump and restoring it')
parser.add_argument('action', metavar='action', type=str, help='backup or restore')
parser.add_argument('-W', '--password', action='store', dest='password', help='PG password')
parser.add_argument('-w', '--no-password', action='store_true', dest='without_password', help='Empty password')
parser.add_argument('-p', '--port', action='store', dest='port', help='PG port (default: 5432)', default=5432)

parser.add_argument('-v', '--verbose', action='store_true', dest='verbose', help='Verbose mode')

required = parser.add_argument_group('required arguments')
required.add_argument('-H', '--host', action='store', dest='host', help='PG hostname', required=True)
required.add_argument('-d', '--database', action='store', dest='database', help='PG database', required=True)
required.add_argument('-U', '--user', action='store', dest='user', help='PG user', required=True)
required.add_argument('-o', '--path', action='store', dest='path', help='Storage folder', required=True)

args = parser.parse_args()

log_level = logging.ERROR
if args.verbose:
    log_level = logging.INFO

logging.basicConfig(
    filename="migration-blobs.log",
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=log_level,
    datefmt='%Y-%m-%d %H:%M:%S')


if not args.without_password and args.password is None:
    print('The password must be set.')
    exit()

if args.action != 'backup' and args.action != 'restore':
    print(f'Wrong action - {args.action}')
    exit()

if args.without_password:
    url = f'pq://{args.user}@{args.host}:{args.port}/{args.database}'
else:
    url = f'pq://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}'

db = postgresql.open(url)
if db:
    logging.info(f'Connected to {url}')
    logging.info(db.version)
else:
    logging.info('Bad connection')
    exit()

ps = db.prepare('SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1')
if len(ps(SCHEMA)) == 0:
    logging.info(f'CREATE SCHEMA {SCHEMA}')
    db.execute(f'CREATE SCHEMA {SCHEMA}')

ps = db.prepare('SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2')
if len(ps(SCHEMA, TABLE_PY_BLOB)) == 0:
    logging.info(f'CREATE TABLE {SCHEMA}.{TABLE_PY_BLOB}(lo_id serial PRIMARY KEY, pages int)')
    db.execute(f'CREATE TABLE {SCHEMA}.{TABLE_PY_BLOB}(lo_id serial PRIMARY KEY, pages int)')

ps = db.prepare('SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2')
if len(ps(SCHEMA, TABLE_PG_BLOB)) == 0:
    logging.info(f'CREATE TABLE {SCHEMA}.{TABLE_PG_BLOB}(lo_id serial PRIMARY KEY)')
    db.execute(f'CREATE TABLE {SCHEMA}.{TABLE_PG_BLOB}(lo_id serial PRIMARY KEY)')

ps = db.prepare('SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2')
if len(ps(SCHEMA, TABLE_PY_RESTORE)) == 0:
    logging.info(f'CREATE TABLE {SCHEMA}.{TABLE_PY_RESTORE}(lo_id serial PRIMARY KEY)')
    db.execute(f'CREATE TABLE {SCHEMA}.{TABLE_PY_RESTORE}(lo_id serial PRIMARY KEY)')

path = os.path.abspath(args.path)
if not os.path.exists(path):
    os.makedirs(path)
    logging.info(f'Make directories - {path}')


if args.action == 'backup':
    round_number = 0
    while True:
        logging.info(f'New round - {round_number}')
        round_number += 1

        ps = db.prepare(
            'SELECT '
            '   lo.lo_id as lo_id '
            'FROM '
            '           migration.pg_largeobject AS lo '
            'LEFT JOIN  migration.py_largeobject AS m ON lo.lo_id = m.lo_id '
            'WHERE m.lo_id IS NULL '
            'LIMIT $1'
        )

        blob_list = []
        for row in ps.rows(args.chunk):
            blob_list.append(row[0])

        if len(blob_list) == 0:
            ps = db.prepare(
                'SELECT '
                '   lo.loid as loid '
                'FROM '
                '           pg_catalog.pg_largeobject AS lo '
                'LEFT JOIN  migration.py_largeobject AS m ON lo.pageno = 0 AND lo.loid = m.lo_id '
                'WHERE lo.pageno = 0 AND m.lo_id IS NULL '
                'LIMIT $1')

            for row in ps.rows(args.chunk):
                blob_list.append(row[0])

        logging.info(f'Blob list - {len(blob_list)}')

        blob_set = set(blob_list)
        logging.info(f'Blob set - {len(blob_set)}')

        if len(blob_set) == 0:
            print("All blobs exported")
            break

        for blob in blob_set:
            ps = db.prepare(
                'SELECT loid, pageno, encode(data, \'hex\') '
                'FROM pg_catalog.pg_largeobject '
                'WHERE loid = $1 '
                'ORDER BY pageno ASC')

            chunk = str(blob)[0:3]
            path_with_folder = f'{path}/{chunk}'
            if not os.path.exists(path_with_folder):
                os.makedirs(path_with_folder)
                logging.info(f'Make directories - {path_with_folder}')

            path_filename = f'{path_with_folder}/{blob}.sql'
            logging.info(f'Open file {path_filename}')
            try:
                count = 0
                with open(path_filename, 'w') as dump_file:
                    dump_file.write(f"SELECT pg_catalog.lo_create('{blob}');\n")
                    dump_file.write(f"SELECT pg_catalog.lo_open('{blob}', '{RIGHTS}');\n")
                    for row in ps.rows(blob):
                        loid, pageno, data = row
                        logging.info(f'Save dump {loid}:{pageno}')
                        dump_file.write(f"SELECT pg_catalog.lowrite(0, '\\x{data}');\n")
                        count += 1
                    dump_file.write("SELECT pg_catalog.lo_close(0);\n")

                logging.info(f'Save progress {loid}, count: {count}')
                db.execute(f"INSERT INTO migration.py_largeobject(lo_id, pages) VALUES ({loid}, {count})")
            except OSError as e:
                logging.error(f'Got OSError Exception - {e}')
            except BaseException as e:
                logging.error(f'Got Exception - {e}')
    logging.info(f'Finish backup all blobs')

if args.action == 'restore':
    blobs = dict()
    ps = db.prepare(f'SELECT lo_id FROM {SCHEMA}.{TABLE_PY_RESTORE}')

    for row in ps.rows():
        blobs[row[0]] = True

    for dir_path, dir_names, filenames in os.walk(args.path):
        for filename in filenames:
            if filename.endswith(".sql"):
                path_filename = os.path.join(dir_path, filename)
                logging.info(f'Found dump file - {path_filename}')
                if os.path.getsize(path_filename) > 0:
                    try:
                        loid = int(os.path.splitext(filename)[0])
                        if loid not in blobs:
                            logging.info(f'Uploading dump file - {path_filename}')
                            with open(path_filename, 'r') as dump_file:
                                with db.xact('SERIALIZABLE'):
                                    for line in dump_file:
                                        db.execute(line)
                                    db.execute(f"INSERT INTO {SCHEMA}.{TABLE_PY_RESTORE}(lo_id) VALUES ({loid})")
                        else:
                            logging.info(f'Skip dump file - {path_filename} - already uploaded')
                    except OSError as e:
                        logging.error(f'Got OSError Exception - {e}')
                    except BaseException as e:
                        logging.error(f'Got Exception - {e}')
                else:
                    logging.error(f'Empty dump file - {path_filename}')
    logging.info(f'Finish restore all blobs')
