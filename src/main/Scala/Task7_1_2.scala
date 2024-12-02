import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object Task7_1_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("T7.1.2")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "trainLibSVM.csv")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")



    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics_2 = new BinaryClassificationMetrics(scoreAndLabels)

    val f1Score = metrics_2.fMeasureByThreshold
    f1Score.collect.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val auROC = metrics_2.areaUnderROC()

    println(s"Area under ROC = $auROC")

    val auPRC = metrics_2.areaUnderPR
    println(s"Area under precision-recall curve = $auPRC")

    val beta = 0.5
    val fScore = metrics_2.fMeasureByThreshold(beta)
    fScore.collect.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    val precision = metrics_2.precisionByThreshold
    precision.collect.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")

      model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
      val sameModel = LogisticRegressionModel.load(sc,
        "target/tmp/scalaLogisticRegressionWithLBFGSModel")

}
  }


}
