import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
/*
Return all customer pairs IDs (C1 and C2) from T3 such that
a. C1 is younger in age than customer C2 and
b. C1 spent in total more money than C2 but bought less items.
T3: ID, name, age, total, num_items
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
    val df = spark.read.csv("T3.csv").toDF("ID", "name", "age", "total", "num_items")
    df.show()
    df.createOrReplaceTempView("T3")

    //a. C1 is younger in age than customer C2 and
    //b. C1 spent in total more money than C2 but bought less items.

    val sqlDF = spark.sql("SELECT c1.ID AS C1ID, c2.ID AS C2ID, c1.age AS Age1, c2.age AS Age2, c1.total AS TotalAmount1, c2.total AS TotalAmount2, c1.num_items AS TotalItemCount1, c2.num_items AS TotalItemCount2 " +
      "FROM T3 c1 JOIN T3 c2 " +
      "ON c1.ID != c2.ID " +
      "WHERE c1.age < c2.age AND c1.total > c2.total AND c2.num_items < c1.num_items")

    sqlDF.show()
    // Define the temporary output directory
    val tempDir = "T4Result_temp"
    // Save as CSV in a single partition without header
    sqlDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .csv(tempDir)

    // Locate the part file in the temporary directory
    val tempFile = new File(tempDir).listFiles().filter(_.getName.startsWith("part-")).head

    // Define the target file path
    val targetPath = Paths.get("T4.csv")

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
