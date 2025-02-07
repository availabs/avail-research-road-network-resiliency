import argparse
import os
import re
import sys

import dotenv
from sqlalchemy import create_engine

### FIXME: Use editable modules
this_dir = os.path.dirname(
    os.path.abspath(__file__)
)

local_modules_path = os.path.join(this_dir, '..')

if local_modules_path not in sys.path:
    sys.path.append(local_modules_path)

from src.utils.db_helpers import load_analysis_dir_into_postgres

env_path = os.path.join(this_dir, '../config/.env')

def get_database_engine():
    pg_creds = dotenv.dotenv_values(env_path)

    user = f"{pg_creds['PGUSER']}:{pg_creds['PGPASSWORD']}"
    mach = f"{pg_creds['PGHOST']}:{pg_creds['PGPORT']}"
    pgdb = pg_creds['PGDATABASE']

    # This code is too green and untested to be dropping database schemas without guardrails.
    if pgdb != 'avail_rnr':
        raise BaseException('INVARIANT BROKEN: until further testing, PGDATABASE MUST equal "avail_rnr".')

    # Connect to the PostGIS database
    postgres_engine = create_engine(
        f"postgresql://{user}@{mach}/{pgdb}",
        connect_args={'connect_timeout': (3600 * 96)}, # 96 hours == 4 days
    )

    return postgres_engine


def main(db_conn, output_dir, clean_schema=False):
    geoid_pattern = re.compile(r'(\d+)[/]?$')
    geoid = geoid_pattern.search(output_dir).group(1)

    postgres_schema = f'e001_001_{geoid}'

    load_analysis_dir_into_postgres(
        db_conn=db_conn,
        postgres_schema=postgres_schema,
        output_dir=output_dir,
        clean_schema=clean_schema,
        single_transaction=True
    )

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Load data from an analysis output directory into Postgres.'
    )

    parser.add_argument(
        '-d',
        '--dir',
        type=str,
        help='Path to the redundancy analysis directory.',
        required=True
    )

    parser.add_argument(
        '--clean_schema',
        action='store_true',
        default=False,
        help='Flag to determine whether to first DROP the postgres_schema.'
    )

    args = parser.parse_args()

    postgres_engine=get_database_engine()

    with postgres_engine.connect() as db_conn:
        main(
            db_conn=db_conn,
            output_dir=args.dir,
            clean_schema=args.clean_schema
        )