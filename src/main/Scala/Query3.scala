/*
Query 3: Given the PEOPLE_WITH_HANDSHAKE_INFO dataset, count how many
individuals are within a 6-unit range of each "activated" person (those with
"HANDSHAKE" = "yes"). Return pairs in the format (connect-i, count-of-close-contacts)
for each initially “activated” individual, where count-of-close-contacts indicates the number
  of nearby people. (12 points)
*/



import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Query3 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query 3")

    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("TESTPEOPLE_WITH_HANDSHAKE_INFO.csv")
    val data: RDD[(Int, Int, Int, String, Int, String, String)] = lines.map {
      line =>
        val Array(id, x, y, name, age, email, activated) = line.split(",")

        (id.toInt, x.toInt, y.toInt, name, age.toInt, email, activated)
    }

    def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
      math.sqrt(math.pow(x2 - x1, 2) + math.pow(y2 - y1, 2))
    }

    def allDistance(points: RDD[(Int, Int, Int, String, Int, String, String)]): RDD[(Int, Int, Double)] = {
      points.cartesian(points)
        .map { case ((id1, x1, y1, _, _, _, _), (id2, x2, y2, _, _, _, _)) =>
          val distance = euclideanDistance(x1.toDouble, y1.toDouble, x2.toDouble, y2.toDouble)
          (id1, id2, distance)
        }
    }


    val distances: RDD[(Int, Int, Double)] = allDistance(data)

    def lessThan6(ids: RDD[(Int, Int, Double)]): RDD[(Int, Int)] = {
      ids.filter { case (id1, id2, distance) => distance <= 6 }
        .map { case (id1, id2, distance) => (id1, id2) }
    }

    val distanceLessThan6: RDD[(Int, Int)] = lessThan6(distances)
    val neighborsGroup: RDD[(Int, Iterable[Int])] = distanceLessThan6.groupByKey()


    def getActiveList(list: RDD[(Int, Iterable[Int])]): RDD[(Int, Int, Int, String, Int, String, String, Iterable[Int])] = {
      data.map(point => (point._1, point)) // Convert data to (id, point) format for joining
        .join(neighborsGroup) // Join on the id
        .map { case (id, (point, neighbors)) =>
          (id, point._2, point._3, point._4, point._5, point._6, point._7, neighbors)
        }
    }

    val activeList = getActiveList(neighborsGroup)

    def activeFilter(list: RDD[(Int, Int, Int, String, Int, String, String, Iterable[Int])]): RDD[(Int, Int, Int, String, Int, String, String, Iterable[Int])] = {
      list.filter { case (id, x, y, name, age, email, activated, neighbors) =>
        activated == "yes"
      }
    }

    val activeListFiltered = activeFilter(activeList)


    val PeopleGroupToCount = activeListFiltered.map {
      case (id, x, y, name, age, email, activated, neighbors) => {
        (id, x, y, name, age, email, activated, neighbors.size)
      }
    }

    PeopleGroupToCount.foreach(println)

    sc.stop()
  }
}
