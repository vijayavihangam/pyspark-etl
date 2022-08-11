from Driver import HummerDriver


def read_source_file(dao_name, src_path, tgt_tbl, sql_query, delimiter, exec_flag):
    print('source files')
    spark = HummerDriver.get_spark_session()
    if delimiter=='NA':
        file_df = spark.read.format("csv").option("header", "true").load("/Users/vijay/Downloads/mock_data/MOCK_DATA.csv")
        active_men_df = file_df.filter("gender == 'Male'").filter("active == 'true'")
        active_men_df.show()
        active_men_df.write.mode('overwrite').parquet("/Users/vijay/Downloads/active_men")
        # active_men_df.write.mode('overwrite').insertInto(tgt_tbl)
    else:
        file_df = spark.read.format("csv").option("header", "true").option("delimiter",'|').load("/Users/vijay/Downloads/mock_data/MOCK_DATA.csv")
        active_men_df = file_df.filter("gender == 'Male'").filter("active == 'true'")
        active_men_df.show()
        active_men_df.write.mode('overwrite').parquet("/Users/vijay/Downloads/active_men")
        #active_men_df.write.mode('overwrite').insertInto(tgt_tbl)
