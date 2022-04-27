from pyspark.sql import *
from pyspark.sql.functions import * 
spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
import urllib
//'''altrnate way is creating .env then same export those variable for security purpose'''
from config import config
ACCESS_KEY = config.acceskey
SECRET_KEY = config.secretkey
Bucket_name = config.bucketname

ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = Bucket_name
MOUNT_NAME = "s3data"
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

//to see s3 files
display(dbutils.fs.ls("/mnt/s3data"))

data = "dbfs:/mnt/s3data/transaction.csv"
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# df.show(5)
df.createOrReplaceTempView("tab")
res = spark.sql("select * from tab where state='NJ'")
res.show()

//mounting complete now further etl and joins are in different file for ease of understanding
