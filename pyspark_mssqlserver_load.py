from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *

 

jars = [
    "/home/herculano/dev/java/jars/mssql-jdbc-9.2.0.jre8.jar"
]


conf = (
    SparkConf()
    .setAppName("MSSQLServer")
    .set("spark.driver.extraClassPath", ":".join(jars))
)

'''
    .set("spark.executor.extraJavaOptions", "-Djdk.tls.client.protocols=TLSv1.2 -Dhttps.protocols=TLSv1.2")
    .set("spark.driver.extraJavaOptions", "-Djdk.tls.client.protocols=TLSv1.2 -Dhttps.protocols=TLSv1.2")
'''

sc = SparkContext(conf=conf).getOrCreate()
sqlContext = SQLContext(sc)

# .option("url", f"jdbc:sqlserver://db.captalyx.com.br:26498;database=Captalys_OP;sslProtocol=TLSv1.2;integratedSecurity=false;encrypt=false;trustServerCertificate=false") \
    #.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \              
df = sqlContext.read \
               .format("jdbc") \
               .option("url", f"jdbc:sqlserver://db..com.br:26498;database=Captalys_OP;encrypt=false;trustServerCertificate=false") \
               .option("dbtable", "(select top 100 * from estoque.tb_EstoqueWexTemp) foo") \
               .option("user", '') \
               .option("password", '') \
               .load()

df.printSchema()
df.show()