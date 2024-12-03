import org.apache.spark.sql.SparkSession
/*
Group the Purchases in T1 by customer ID only for young customers between
18 and 25 years of age. For each group report the customer ID, their age, and total number
of items that this person has purchased, and total amount spent by the customer. Store the
result as T3.
 */

object Task3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("T3")
      .config("spark.master", "local")
      .getOrCreate()
    val df = spark.read.csv("Customers.csv")
    df.show()
    df.createOrReplaceTempView("customers")
    val df2 = spark.read.csv("Purchases.csv")
    df2.show()
    df2.createOrReplaceTempView("purchases")

    val sqlDF = spark.sql("SELECT c._c0 as ID, c._c1 as Name, c._c2 as Age, SUM(p._c2) as Total, SUM(p._c3) as Num_Items FROM customers c JOIN purchases p ON c._c0 = p._c1 WHERE c._c2 < 25 group by c._c0, c._c1, c._c2")

  }

}