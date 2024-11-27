import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

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

    val peopleList: RDD[(Int, Double, Double)] = people.map { line =>
      val Array(id, x, y, name, age, email) = line.split(",")
      (id.toInt, x.toDouble, y.toDouble)
    }

    val activatedList: RDD[(Int, Double, Double)] = activated.map { line =>
      val Array(id, x, y, name, age, email) = line.split(",")
      (id.toInt, x.toDouble, y.toDouble)
    }

    // Broadcast the activated IDs for efficient lookup
    val activatedSet = sc.broadcast(activatedList.collect().toSet)

    // Create a new RDD with a flag for activation status
    val flaggedPeopleList: RDD[(Int, Double, Double, Boolean)] = peopleList.map { case (id, x, y) =>
      val isActivated = activatedSet.value.contains(id, x, y) // Use the broadcasted value
      (id, x, y, isActivated)
    }

    flaggedPeopleList.collect().foreach {
      case (id, x, y, isActivated) =>
        println(s"ID: $id, Activated: $isActivated")
    }

    /*val peopleAtRange: RDD[Int] = flaggedPeopleList.flatMap(
      bin => {
        val peopleIterator: Iterable[(Double, Double, Boolean, Int)] = bin._2
        val inactivatedPeopleSet: mutable.Set[Int] = new mutable.HashSet()
        val activatedPeople: Iterable[(Double, Double, Boolean, Int)] = peopleIterator.filter(value => value._3)
        val inactivatedPeople: Iterable[(Double, Double, Boolean, Int)] = peopleIterator.filter(value => !value._3)

        for (activatedIndividual <- activatedPeople) {
          for (inactivatedPerson <- inactivatedPeople) {
            if (!inactivatedPeopleSet.contains(inactivatedPerson._4) && euclideanDistance(activatedIndividual._1, activatedIndividual._2, inactivatedPerson._1, inactivatedPerson._2) < 6) {
              inactivatedPeopleSet.add(inactivatedPerson._4)
            }
          }
        }
        inactivatedPeopleSet
      }
    )

    val uniquePeopleIds = peopleAtRange.distinct()

    // Collect and print results
    val result = uniquePeopleIds.collect()
    println(s"Unique People IDs within 6 units: ${result.mkString(", ")}")
    //make empty peopleId list and check if each people id has been used, if yes, don't add again, if no, use it and add to list
    //crossList.collect().foreach { case (id1, peopleIds) => println(s"Activated ID $id1 has nearby People IDs: ${peopleIds.mkString(", ")}") }
    */
    sc.stop()

  }
}
