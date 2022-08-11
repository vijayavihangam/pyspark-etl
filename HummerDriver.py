import sys

from pyspark.sql import SparkSession
import csv
from zone import CurzToConzDAO, RawzToCurzDAO, FileToInboundDelimitedDAO

conf = str(sys.argv[1])


def get_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def read_config():
    #with open("/Users/vijay/Downloads/hummer_config.csv") as f:
    with open(conf) as f:
        file_data = csv.reader(f)
        headers = next(file_data)
        d = [dict(zip(headers, i)) for i in file_data]
        csv_id = 0
        for row in d:
            csv_id = csv_id + 1
            arg = {}
            config = {}
            dao_name = row['dao_name']
            src_path = row['src_path']
            tgt_tbl = row['tgt_tbl']
            sql_query = row['sql_query']
            delimiter = row['delimiter']
            exec_flag = row['exec_flag']

            if dao_name == 'file_to_rawz_delimited_dao' and exec_flag.lower() == 'y':
                FileToInboundDelimitedDAO.read_source_file(dao_name, src_path, tgt_tbl, sql_query, delimiter, exec_flag)

            if dao_name == 'rawz_to_curz_dao' and exec_flag.lower() == 'y':
                RawzToCurzDAO.read_src_tbl(dao_name, src_path, tgt_tbl, sql_query, delimiter, exec_flag)

            if dao_name == 'curz_to_conz_dao' and exec_flag.lower() == 'y':
                CurzToConzDAO.read_src_tbl(dao_name, src_path, tgt_tbl, sql_query, delimiter, exec_flag)

    return config


def main():
    read_config()
    get_spark_session()


if __name__ == '__main__':
    main()
