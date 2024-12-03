import org.apache.spark.sql.SparkSession
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
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
    val df = spark.read.schema("_c0 INTEGER, _c1 INTEGER, _c2 INTEGER,_c3 INTEGER, _c4 STRING").csv("TaskOutputs/T1.csv")
    //df.show()
    df.createOrReplaceTempView("table")
    val sqlDF = spark.sql("SELECT _c3, min(_c2) as min, max(_c2) as max, percentile_approx(_c2, 0.5) as median from table group by _c3")
    sqlDF.show()
    val tempDir = "T2Result_temp"

    // Save as CSV in a single partition without header
    sqlDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .csv(tempDir)

    // Locate the part file in the temporary directory
    val tempFile = new File(tempDir).listFiles().filter(_.getName.startsWith("part-")).head

    // Define the target file path
    val targetPath = Paths.get("T2.csv")

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
