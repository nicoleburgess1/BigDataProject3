import org.apache.spark.sql.SparkSession
/*
Task 2.1) Filter out (drop) the Purchases from P with a total purchase amount above $600.
Store the result as T1.
part-00000-f17e3133-8bd4-45b3-8e3c-5c7343722eb6-c000.csv
 */
import org.apache.spark.sql.SparkSession

object Task1 {

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder()
      .appName("DataFrameExample")
      .master("local[*]")
      .getOrCreate()
    spark.catalog.clearCache()

    import spark.implicits._

    val data = Seq((1, "Alice", 23), (2, "Bob", 30), (3, "Catherine", 25))
    val df = data.toDF("id", "name", "age")

    df.show()

    spark.stop()*/

    val spark = SparkSession.builder()
      .appName("T1")
      .config("spark.master", "local")
      .getOrCreate()
    spark.catalog.clearCache()
    val df = spark.read.csv("Purchases.csv")
    df.show()

    // Register DataFrame as a SQL temporary view
    df.createOrReplaceTempView("purchases")

    // SQL query
    val sqlDF = spark.sql("SELECT * FROM purchases WHERE _c1 > 600")
    sqlDF.show()
    sqlDF.coalesce(1).write.csv("T1Result.csv")

    // Save result
    //sqlDF.write.json("path/to/output/json")

    // Stop the Spark session
    spark.stop()
  }
}
