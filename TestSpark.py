from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([StructField('Year', IntegerType(), True),
                     StructField('Industry_aggregation_NZSIOC', StringType(), True),
                     StructField('Industry_code_NZSIOC', StringType(), True),
                     StructField('Industry_name_NZSIOC', StringType(), True),
                     StructField('Units', StringType(), True),
                     StructField('Variable_code', StringType(), True),
                     StructField('Variable_name', StringType(), True),
                     StructField('Variable_category', StringType(), True),
                     StructField('Value', StringType(), True),
                     StructField('Industry_code_ANZSIC06', StringType(), True)
                     ])
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df = spark.read.csv("/Users/dileepreddy/Downloads/testdata1.csv", header=True, schema=schema)
#url = 'https://raw.githubusercontent.com/pydata/pydata-book/master/ch09/stock_px.csv'
#f = spark.read.csv(url)
#df.printSchema()
df.show(5)