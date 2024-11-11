import org.apache.spark.sql.SparkSession
/*
Task 2.1) Filter out (drop) the Purchases from P with a total purchase amount above $600.
Store the result as T1.
 */
import org.apache.spark.sql.SparkSession

object Task1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq((1, "Alice", 23), (2, "Bob", 30), (3, "Catherine", 25))
    val df = data.toDF("id", "name", "age")

    df.show()

    spark.stop()
  }
  /*val spark = SparkSession.builder()
    .appName("T1")
    .config("spark.master", "local")
    .getOrCreate()
  val df = spark.read.csv("Purchases.csv")
  df.show()

  // Register DataFrame as a SQL temporary view
  df.createOrReplaceTempView("people")

  // SQL query
  val sqlDF = spark.sql("SELECT name, age FROM people WHERE age > 24")
  sqlDF.show()

  // Save result
  sqlDF.write.json("path/to/output/json")

  // Stop the Spark session
  spark.stop()*/

}
