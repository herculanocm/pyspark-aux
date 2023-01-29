from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *


aws_access_key_id=''
aws_secret_access_key=''
aws_session_token=''

config = {
    'aws_access_key': aws_access_key_id,
    'aws_secret_key': aws_secret_access_key,
    'aws_session_token': aws_session_token,
    'aws_region': 'sa-east-1',
    'aws_bucket': 'captalys-analytics-temporary',
    'redshift_user': '',
    'redshift_pass': '',
    'redshift_port': 5439,
    'redshift_db': 'datalake_dw',
    'redshift_host': 'asgard-redshift-production..sa-east-1.redshift.amazonaws.com',
}
 

jars = [
    "/home/herculano/dev/py/redshift_spark/redshift-jdbc42-2.1.0.7.jar"
]

conf = (
    SparkConf()
    .setAppName("S3 with Redshift")
    .set("spark.driver.extraClassPath", ":".join(jars))
    .set("spark.hadoop.fs.s3a.access.key", config.get('aws_access_key'))
    .set("spark.hadoop.fs.s3a.secret.key", config.get('aws_secret_key'))
    .set("spark.hadoop.fs.s3a.session.token", config.get('aws_session_token'))
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("com.amazonaws.services.s3.enableV4", True)
    .set("spark.hadoop.fs.s3a.endpoint", f"s3-{config.get('region')}.amazonaws.com")
    .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
)
sc = SparkContext(conf=conf).getOrCreate()
sqlContext = SQLContext(sc)


schema = 'captalys_trusted'
table = 'platform_cessao_master_cedente_processo'


df = sqlContext.read \
               .format("jdbc") \
               .option("url", f"jdbc:redshift://{config.get('redshift_host')}:{config.get('redshift_port')}/{config.get('redshift_db')}") \
               .option("dbtable", "(select * from captalys_trusted.platform_cessao_master_cedente_processo limit 100) foo") \
               .option("user", config.get('redshift_user')) \
               .option("password", config.get('redshift_pass')) \
               .load()

df.show()