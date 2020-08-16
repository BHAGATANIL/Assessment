import sqlite3
from pyspark.sql import SparkSession


"""
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
"""
#directors2= sqlContext.read.format("jdbc").options(url ="jdbc:sqlite:c:/sqlite/movies.db", driver="org.sqlite.JDBC", dbtable="directors").load()

"""
directors= sqlCtx.read\
    .format("jdbc")\
    .options(url ="jdbc:sqlite:c:/sqlite/movies.db", driver="org.sqlite.JDBC", dbtable="directors").load()
directors.show()
"""

url = "jdbc:sqlite:C:/sqlite/movies.db"
con=sqlite3.connect("url")

""""
db_path = 'C:/sqlite/movies.db'
query = 'SELECT * from directors'

db_path = 'C:/sqlite/movies.db'
conn = sqlite3.connect(db_path)
"""

spark: SparkSession = SparkSession \
    .builder \
    .appName("Movies") \
    .master("local") \
    .config("spark.sqlite.wa  rehouse.dir", "C:/sqlite/") \
    .getOrCreate();



db_path = 'jdbc:sqlite:C:/sqlite/movies.db'
conn = sqlite3.connect("db_path")

url ="jdbc:sqlite:c:/sqlite/movies.db"
#cur_con=conn.cursor()
"""
directors= spark.read\
    .format("jdbc")\
    .options(url ="jdbc:sqlite:c:/sqlite/movies.db", driver="org.sqlite.JDBC", dbtable="directors").load()
#directors.show()
"""


directors=spark.read \
    .format("jdbc")\
    .option("url", "con")\
    .option("username","root")\
    .option("driver", "org.sqlite.JDBC")\
    .option("password", "password")\
    .option("dbtable","directors").load()

directors.show()