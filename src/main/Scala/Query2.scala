import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

/*
Given the same setup in Query 1, however, return the unique identifiers pi.id of
all individuals in PEOPLE who were within 6 units of any person in ACTIVATED,
ensuring each person appears only once in the result with duplicates removed. That is,
even if a person pi was close to two different "activated" people, return that person pi only
once.
 */

object Query2 {
  def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    math.sqrt(math.pow(x2 - x1, 2) + math.pow(y2 - y1, 2))
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Task1Query2").setMaster("local")
    val sc = new SparkContext(conf)
    val peopleDF = sc.textFile("Part1/TESTPEOPLE.csv")
    val activatedDF = sc.textFile("Part1/TESTACTIVATED.csv")
    val peopleData = peopleDF.map(line => {
      val fields = line.split(",")
      val id = fields(0).toInt
      val x = fields(1).toInt
      val y = fields(2).toInt
      val name = fields(3)
      val age = fields(4).toInt
      val email = fields(5)
      (id, x, y, name, age, email)
    })
    val activatedData = activatedDF.map(line => {
      val fields = line.split(",")
      val id = fields(0).toInt
      val x = fields(1).toInt
      val y = fields(2).toInt
      val name = fields(3)
      val age = fields(4).toInt
      val email = fields(5)
      (id, x, y, name, age, email)
    })
    val activationRange = 6.0
    val result = peopleData.cartesian(activatedData)
      .filter { case (person, activatedPerson) =>
        Math.sqrt(Math.pow(person._2 - activatedPerson._2, 2) + Math.pow(person._3 - activatedPerson._3, 2)) <= activationRange
      }
      .map { case (person, _) => person._1 }.distinct().collect()
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = "Task1Query2"
    val outputDir = new Path(outputPath)
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true)
    }
    sc.parallelize(result).repartition(1).saveAsTextFile(outputPath)

    sc.stop()
  }
}
