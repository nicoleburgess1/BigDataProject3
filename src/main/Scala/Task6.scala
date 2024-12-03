import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
/*

Data Preparation 2: (Randomly) split Dataset into two subsets, namely, Trainset
and Testset, such that Trainset contains 80% of Dataset and Testset the remaining 20%. (2
points)

 */


object Task6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("T6")
      .config("spark.master", "local")
      .getOrCreate()
    val df = spark.read.csv("input/ecommerce_customer_data_large.csv")
    df.show()
    df.createOrReplaceTempView("customers")
    val Array(train, test) = df.randomSplit(Array(0.8, 0.2))

    println("Training data:")
    train.show()

    println("Testing data:")
    test.show()

    val tempDir = "T6_1_Result_temp"
    // Save as CSV in a single partition without header
    train.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .csv(tempDir)

    // Locate the part file in the temporary directory
    val tempFile = new File(tempDir).listFiles().filter(_.getName.startsWith("part-")).head

    // Define the target file path
    val targetPath = Paths.get("train.csv")

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



    val tempDir_2 = "T6_2_Result_temp"
    // Save as CSV in a single partition without header
    test.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .csv(tempDir_2)

    // Locate the part file in the temporary directory
    val tempFile_2 = new File(tempDir_2).listFiles().filter(_.getName.startsWith("part-")).head

    // Define the target file path
    val targetPath_2 = Paths.get("test.csv")

    // Move and rename the file
    Files.move(tempFile_2.toPath, targetPath_2, StandardCopyOption.REPLACE_EXISTING)


    // Delete the temporary directory
    deleteDirectoryRecursively(new File(tempDir_2))
  }

}
