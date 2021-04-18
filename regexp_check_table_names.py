import hashlib
import os.path
import re
from pathlib import Path
from typing import Generator

BASE_DIR_PATH = "/Users/nikolaysokolov"
DIR_PATH_WITH_SQL_FILES = os.getenv('DIR_PATH_WITH_SQL_FILES',
                                    f"{BASE_DIR_PATH}/git/tableau_airflow/services/airflow/dags/spark-submit/sql/storage")
REGEXP_DIR_PATH = os.getenv('REGEXP_DIR_PATH', f"{BASE_DIR_PATH}/tmp/uchi.ru/regexp-parse-out/")
SPARK_DIR_PATH = os.getenv('SPARK_DIR_PATH', f"{BASE_DIR_PATH}/tmp/uchi.ru/spark-parse-out/")

FILE_EXTENSION = "*.sql"
CH_DB_PREFIX = ('raw', 'storage', 'tmp')

# Double quotes are not counted in DB & table names. Spark does not support double quotes in DB & table names
re_tables = re.compile(r'\s+(?:from|join)\s+([a-z]\w*\.[a-z]\w*)', re.M | re.I)
re_ch_db_tables = re.compile(rf'\s+(?:from|join)\s+((?:{"|".join(CH_DB_PREFIX)})_[a-z]\w*\.[a-z]\w*)', re.M | re.I)


def write_table_names_to_files():
    for filepath in get_path_sql_files():
        table_names = sorted(list(get_table_names_from_sql_file(filepath)))
        file_name_with_table_names = "__".join(filepath.parts[-3:])
        filepath_with_table_names = os.path.join(REGEXP_DIR_PATH, file_name_with_table_names)
        write_table_names_to_file(filepath_with_table_names, table_names)


def check_all_files_hash_equality():
    files_hash_not_matched_amt = files_amt = 0
    sql_total_files = sum(1 for _ in get_path_sql_files())

    for files_amt, hive_file in enumerate(get_path_sql_files(SPARK_DIR_PATH)):
        if is_file_exist(REGEXP_DIR_PATH, hive_file.name):
            regexp_file = os.path.join(REGEXP_DIR_PATH, hive_file.name)
            hive_file_hash, regexp_file_hash = get_file_checksum(hive_file), get_file_checksum(regexp_file)

            if hive_file_hash != regexp_file_hash:
                files_hash_not_matched_amt += 1
                print(hive_file.name)

    print(f"\n{files_amt} files were checked out of {sql_total_files}."
          f"\n{files_hash_not_matched_amt} file hashes did not match.")


def get_path_sql_files(dir_path=DIR_PATH_WITH_SQL_FILES, file_extension=FILE_EXTENSION) -> Generator:
    return Path(dir_path).rglob(file_extension)


def get_table_names_from_sql_file(sql_filepath) -> set:
    with open(sql_filepath, 'r') as f:
        data = f.read()
        all_matches = re_ch_db_tables.findall(data)
        return set(all_matches)


def write_table_names_to_file(filepath, table_names):
    with open(filepath, 'w') as f:
        for table_name in table_names:
            f.write("%s\n" % table_name)


def is_file_exist(filepath, filename) -> bool:
    return os.path.exists(os.path.join(filepath, filename))


def get_file_checksum(filepath) -> str:
    return hashlib.md5(open(filepath, 'rb').read()).hexdigest()


if __name__ == '__main__':
    write_table_names_to_files()
    check_all_files_hash_equality()
