object ohe {
  import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
  import org.apache.spark.sql.SparkSession

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("T6")
      .config("spark.master", "local")
      .getOrCreate()
    // Category DataFrame
    val df = spark.read.csv("train.csv")
      .toDF("CustomerID", "PurchaseDate", "ProductCategory", "ProductPrice", "Quantity", "TotalPurchaseAmount", "PaymentMethod", "CustomerAge", "Returns"," CustomerName", "Age", "Gender", "Churn")
    // StringIndexer to convert category strings to numerical indices
    val indexer = new StringIndexer()
      .setInputCol("ProductCategory")
      .setOutputCol("ProductCategoryIndex")
    val indexed = indexer.fit(df).transform(df)
    // OneHotEncoder to convert indexed categories to one-hot vectors
    val encoder = new OneHotEncoder()
      .setInputCol("ProductCategoryIndex")
      .setOutputCol("ProductCategoryEncoded")
    val encoded = encoder.fit(indexed).transform(indexed)
    //encoded.show()

    //Gender
    val indexer2 = new StringIndexer()
      .setInputCol("Gender")
      .setOutputCol("GenderIndex")
    val indexed2 = indexer2.fit(encoded).transform(encoded)
    // OneHotEncoder to convert indexed categories to one-hot vectors
    val encoder2 = new OneHotEncoder()
      .setInputCol("GenderIndex")
      .setOutputCol("GenderEncoded")
    val encoded2 = encoder2.fit(indexed2).transform(indexed2)
    //encoded2.show()

    //Payment Method
    val indexer3 = new StringIndexer()
      .setInputCol("PaymentMethod")
      .setOutputCol("PaymentMethodIndex")
    val indexed3 = indexer3.fit(encoded2).transform(encoded2)
    // OneHotEncoder to convert indexed categories to one-hot vectors
    val encoder3 = new OneHotEncoder()
      .setInputCol("PaymentMethodIndex")
      .setOutputCol("PaymentMethodEncoded")
    val encoded3 = encoder3.fit(indexed3).transform(indexed3)
    val finalDf = encoded3
      .drop("ProductCategory", "ProductCategoryIndex", "Gender", "GenderIndex", "PaymentMethod", "PaymentMethodIndex")

    finalDf.show()
  }
}
