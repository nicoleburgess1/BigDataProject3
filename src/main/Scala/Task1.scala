import org.apache.spark.sql.SparkSession
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
/*
Task 2.1) Filter out (drop) the Purchases from P with a total purchase amount above $600.
Store the result as T1.
part-00000-f17e3133-8bd4-45b3-8e3c-5c7343722eb6-c000.csv
 */
import org.apache.spark.sql.SparkSession

object Task1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("T1")
      .config("spark.master", "local")
      .getOrCreate()
    spark.catalog.clearCache()
    val df = spark.read.csv("Part2/Purchases.csv")
    df.show()

    // Register DataFrame as a SQL temporary view
    df.createOrReplaceTempView("purchases")

    // SQL query
    val sqlDF = spark.sql("SELECT * FROM purchases WHERE _c1 < 600")
    sqlDF.show()
    // Define the temporary output directory
    val tempDir = "T1Result_temp"

    // Save as CSV in a single partition without header
    sqlDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .csv(tempDir)

    // Locate the part file in the temporary directory
    val tempFile = new File(tempDir).listFiles().filter(_.getName.startsWith("part-")).head

    // Define the target file path
    val targetPath = Paths.get("T1.csv")

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
