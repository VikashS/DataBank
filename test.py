from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.window import Window

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()
    loginData = spark.read.csv("G:/Interview/powercell/aws/*/event=login/login.csv.gz", header=True)
    trx = spark.read.csv("G:/Interview/powercell/aws/*/event=transaction/transaction.csv.gz", header=True)
    modloginDf = loginData.withColumn("login_date", f.date_format(f.col("timestamp"), "dd-MM-yyyy"))
    loginDf = modloginDf.drop_duplicates(modloginDf.columns)
    trxDf = trx.withColumnRenamed("timestamp", "timestamp_txn").withColumnRenamed("session_id", "session_id_txn") \
        .withColumnRenamed("account_id", "account_id_txn")

    combinedDataDF = trxDf.join(loginDf, (loginDf.account_id == trxDf.account_id_txn) & (
                loginDf.session_id == trxDf.session_id_txn), "inner")
    column_list = ["country", "login_date"]
    order_list = ["country", "login_date"]
    running_window = Window.partitionBy(column_list) \
        .orderBy(order_list)
    finaldata = combinedDataDF.withColumn("PDUA", f.count('account_id_txn').over(running_window))
    pdu_count_df = finaldata.select("login_date", "country", "PDUA").distinct()
    output_loc = "G:\\Interview\\powercell\\result\\"
    dates_coll = pdu_count_df.select("login_date").distinct().collect()
    for date in dates_coll:
        d = str(date.login_date)
        each_day_df = pdu_count_df.filter(pdu_count_df.login_date == date.login_date)
        filename = "PDAU_" + str(date.login_date) + '.csv'
        each_day_df.toPandas().to_csv(output_loc + filename, sep=',', header=True, index=False)
    print("end")




