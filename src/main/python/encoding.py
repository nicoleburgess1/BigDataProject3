from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("T6") \
    .config("spark.master", "local") \
    .getOrCreate()

# Load your DataFrame
df = spark.read.option("header", "true").csv("C:\\Users\\annie\\OneDrive\\Desktop\\DS503\\BigDataProject3\\input\\ecommerce_customer_data_large.csv")

# Use StringIndexer to convert "Product Category" to numeric indices
indexer = StringIndexer(inputCol="Product Category", outputCol="Product Category Index")
df_indexed = indexer.fit(df).transform(df)

# Use OneHotEncoder to apply one-hot encoding on the indexed column
encoder = OneHotEncoder(inputCol="Product Category Index", outputCol="Product Category Encoded")
df_encoded = encoder.fit(df_indexed).transform(df_indexed)

# Apply StringIndexer to "Gender"
indexer_2 = StringIndexer(inputCol="Gender", outputCol="Gender Index")
df_indexed_2 = indexer_2.fit(df_encoded).transform(df_encoded)

# Use OneHotEncoder to apply one-hot encoding on the indexed "Gender" column
encoder_2 = OneHotEncoder(inputCol="Gender Index", outputCol="Gender Encoded")
df_encoded_2 = encoder_2.fit(df_indexed_2).transform(df_indexed_2)

# Apply StringIndexer to "Payment Method"
indexer_3 = StringIndexer(inputCol="Payment Method", outputCol="Payment Method Index")
df_indexed_3 = indexer_3.fit(df_encoded_2).transform(df_encoded_2)

# Use OneHotEncoder to apply one-hot encoding on the indexed "Payment Method" column
encoder_3 = OneHotEncoder(inputCol="Payment Method Index", outputCol="Payment Method Encoded")
df_encoded_3 = encoder_3.fit(df_indexed_3).transform(df_indexed_3)

# Drop the original columns after encoding
df_encoded_final = df_encoded_3.drop("Product Category", "Product Category Index", "Gender", "Gender Index", "Payment Method", "Payment Method Index")

# Show the final DataFrame
df_encoded_final.show(truncate=False)
