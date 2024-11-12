import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

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
    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")
    val sc = new SparkContext(sparConf)
    val people: RDD[String] = sc.textFile("TESTPEOPLE.csv")
    val activated: RDD[String] = sc.textFile("TESTACTIVATED.csv")

    val peopleList: RDD[(Int, Int, Int, String, Int, String)] = people.map { line =>
      val Array(id, x, y, name, age, email) = line.split(",")
      (id.toInt, x.toInt, y.toInt, name, age.toInt, email)
    }

    val activatedList: RDD[(Int, Int, Int, String, Int, String)] = activated.map { line =>
      val Array(id, x, y, name, age, email) = line.split(",")
      (id.toInt, x.toInt, y.toInt, name, age.toInt, email)
    }

    //for each activated if distance to people is <=6 add people id to list with activated id if not already activated
    val crossList = activatedList.cartesian(peopleList).filter { case ((id1, x1, y1, _, _, _), (id2, x2, y2, _, _, _)) => euclideanDistance(x1, y1, x2, y2) <= 100 }
      .map { case ((id1, _, _, _, _, _), (id2, _, _, _, _, _)) => (id1, id2) }
      .distinct()
      .groupByKey()
      .mapValues(_.toList)
    //make empty peopleId list and check if each people id has been used, if yes, don't add again, if no, use it and add to list
    crossList.collect().foreach { case (id1, peopleIds) => println(s"Activated ID $id1 has nearby People IDs: ${peopleIds.mkString(", ")}") }
    sc.stop()
  }
}
