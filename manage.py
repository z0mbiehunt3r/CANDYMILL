import argparse
import os


def _setup_db(engine_str):
    from candymill.candymill import setup_database
    setup_database(engine_str)


def _check_storage(engine_str, storage_path):
    from candymill.candymill import CandyStorage
    cs = CandyStorage(storage_path, depth=3, width=2, algorithm='sha256', engine_url=engine_str)
    return cs.check_storage_consistency()


def _store_files(engine_str, storage_path, input_path):
    from candymill.candymill import CandyStorage
    cs = CandyStorage(storage_path, depth=3, width=2, algorithm='sha256', engine_url=engine_str)
    for root, dirs, files in os.walk(input_path):
        for fname in files:
            file_path = os.path.join(root, fname)
            file_path = os.path.abspath(file_path)
            yield cs.put(file_path, extension=None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    subs.required = True
    subs.dest = '{createdb,checkstorage,addfiles}'
    db_parser = subs.add_parser('createdb', help='Create DB schema')
    db_parser.set_defaults(func=_setup_db)
    db_parser.add_argument('--engine', required=True, help='DB connection string, eg: "sqlite:///files.sqlite"')
    chk_parser = subs.add_parser('checkstorage', help='Check DB and FS consistency')
    chk_parser.set_defaults(func=_check_storage)
    chk_parser.add_argument('--engine', required=True, help='DB connection string, eg: "sqlite:///files.sqlite"')
    chk_parser.add_argument('--storage', required=True, help='Path to filesystem storage base dir')
    add_parser = subs.add_parser('addfiles', help='Process and store files')
    add_parser.add_argument('--engine', required=True, help='DB connection string, eg: "sqlite:///files.sqlite"')
    add_parser.add_argument('--storage', required=True, help='Path to filesystem storage base dir')
    add_parser.add_argument('--samples', required=True, help='Path to samples to process and store')
    add_parser.set_defaults(func=_store_files)

    args = parser.parse_args()

    if args.func.__name__ == '_setup_db':
        print('Setting up database schema')
        _setup_db(args.engine)
        print('Done!')
    elif args.func.__name__ == '_check_storage':
        print('Analyzing storage, both DB and filesystem levels...')
        consistency = _check_storage(args.engine, args.storage)
        if not consistency.consistent:
            print('WARNING!, there are some discrepancies amongst DB metadata and FS stored files')
            print(f'\nAt DB we have metadata for {consistency.db_count} files, but we have '
                  f'{consistency.fs_count} files stored')
            # todo: metodo para borrar metadatos de ficheros no presentes?
    elif args.func.__name__ == '_store_files':
        print(f'Processing and storing files (skipping already stored) under {os.path.abspath(args.samples)}')
        for res in _store_files(args.engine, args.storage, args.samples):
            if res is None:
                continue
            if res.is_duplicate:
                continue
            print(f'  Processed and stored file {res.id}')
        print('Done!')
