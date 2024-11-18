import org.apache.spark.sql.SparkSession
/*
Return all customer pairs IDs (C1 and C2) from T3 such that
a. C1 is younger in age than customer C2 and
b. C1 spent in total more money than C2 but bought less items.
Store the result as T4 and report it back in the form (C1 ID, C2 ID, Age1, Age2,
TotalAmount1, TotalAmount2, TotalItemCount1, TotalItemCount2) to the client side. (5
points)
 */


object Task4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("T4")
      .config("spark.master", "local")
      .getOrCreate()
    val df = spark.read.csv("Purchases.csv")
    df.show()

  }
}
