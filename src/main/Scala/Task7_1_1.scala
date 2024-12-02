import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

object Task7_1_1 {

  def main(args: Array[String]): Unit = {
    // Load training data in LIBSVM format.
    val spark = SparkSession.builder()
      .appName("T7.1")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext
    val data = MLUtils.loadLibSVMFile(sc, "trainLibSVM.csv")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    val f1Score = metrics.fMeasureByThreshold
    f1Score.collect.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val auROC = metrics.areaUnderROC()

    println(s"Area under ROC = $auROC")

    val auPRC = metrics.areaUnderPR
    println(s"Area under precision-recall curve = $auPRC")

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    fScore.collect.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    val precision = metrics.precisionByThreshold
    precision.collect.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }
  }

}
