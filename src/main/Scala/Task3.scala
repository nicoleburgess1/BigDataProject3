import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
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

    // SQL query
    val sqlDF = spark.sql("SELECT c._c0 as ID, c._c1 as Name, c._c2 as Age, SUM(p._c2) as Total, SUM(p._c3) as Num_Items FROM customers c JOIN purchases p ON c._c0 = p._c1 WHERE c._c2 < 25 group by c._c0, c._c1, c._c2")
    sqlDF.show()
    // Define the temporary output directory
    val tempDir = "T3Result_temp"
    // Save as CSV in a single partition without header
    sqlDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .csv(tempDir)

    // Locate the part file in the temporary directory
    val tempFile = new File(tempDir).listFiles().filter(_.getName.startsWith("part-")).head

    // Define the target file path
    val targetPath = Paths.get("T3.csv")

    // Move and rename the file
    Files.move(tempFile.toPath, targetPath, StandardCopyOption.REPLACE_EXISTING)

    // Delete the temporary directory
    // Recursive function to delete a directory and its contents
    def deleteDirectoryRecursively(file: File): Unit = {
      if (file.isDirectory) {
        file.listFiles().foreach(deleteDirectoryRecursively) // Delete all contents
      }
      file.delete() // Delete the directory itself
    }

    // Delete the temporary directory
    deleteDirectoryRecursively(new File(tempDir))

    spark.stop()

  }

}