import org.apache.spark.sql.SparkSession
/*
Group the Purchases in T1 by the Number of Items purchased. For each group
calculate the median, min and max of total amount spent for purchases in that group.
Report the result back to the client side.

Need to fix the fact that all outputs are the same and add in median
 */
object Task2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("T2")
      .config("spark.master", "local")
      .getOrCreate()
    spark.catalog.clearCache()
    val df = spark.read.schema("_c0 INTEGER, _c1 INTEGER, _c2 INTEGER,_c3 INTEGER, _c4 STRING").csv("T1_test.csv")
    //df.show()
    df.createOrReplaceTempView("table")
    val result = spark.sql("SELECT _c3, min(_c2), max(_c2) from table group by _c3")
    result.show()


    result.write.csv("T2")
    import spark.implicits._

  //df.show()

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
