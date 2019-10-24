# /bin/python3

import os
import argparse
import logging
import postgresql

SCHEMA = 'migration'
TABLE_PY = 'py_largeobject_restore'

parser = argparse.ArgumentParser(description='Make SQL dump and restore')
parser.add_argument('-W', '--password', action='store', dest='password', help='PG password')
parser.add_argument('-w', '--no-password', action='store_true', dest='without_password', help='Empty password')
parser.add_argument('-p', '--port', action='store', dest='port', help='PG port (default: 5432)', default=5432)

required = parser.add_argument_group('required arguments')
required.add_argument('-H', '--host', action='store', dest='host', help='PG hostname', required=True)
required.add_argument('-d', '--database', action='store', dest='database', help='PG database', required=True)
required.add_argument('-U', '--user', action='store', dest='user', help='PG user', required=True)
required.add_argument('-o', '--path', action='store', dest='path', help='Storage folder', required=True)
required.add_argument('-l', '--limit', action='store', type=int, dest='limit', help='Limit workers', required=True)
required.add_argument('-i', '--index', action='store', type=int, dest='index', help='Worker index', required=True)

args = parser.parse_args()

logging.basicConfig(
    filename=f"restore{args.index}.log",
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.ERROR,
    datefmt='%Y-%m-%d %H:%M:%S')

if not args.without_password and args.password is None:
    print('The password must be set.')
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
if len(ps(SCHEMA, TABLE_PY)) == 0:
    logging.info(f'CREATE TABLE {SCHEMA}.{TABLE_PY}(lo_id serial PRIMARY KEY)')
    db.execute(f'CREATE TABLE {SCHEMA}.{TABLE_PY}(lo_id serial PRIMARY KEY)')

ps = db.prepare(f'SELECT lo_id FROM {SCHEMA}.{TABLE_PY}')

blobs = dict()
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
                    if loid % args.limit != args.index:
                        logging.info(f'Skip dump file - {loid} % {args.limit} != {args.index}')
                        continue
                    if loid not in blobs:
                        logging.info(f'Uploading dump file - {path_filename}')
                        with open(path_filename, 'r') as dump_file:
                            with db.xact('SERIALIZABLE'):
                                for line in dump_file:
                                    db.execute(line)
                                db.execute(f"INSERT INTO {SCHEMA}.{TABLE_PY}(lo_id) VALUES ({loid})")
                    else:
                        logging.info(f'Skip dump file - {path_filename} - already uploaded')
                except OSError as e:
                    logging.error(f'Got OSError Exception - {e}')
                except BaseException as e:
                    logging.error(f'Got Exception - {e}')
            else:
                logging.error(f'Empty dump file - {path_filename}')
