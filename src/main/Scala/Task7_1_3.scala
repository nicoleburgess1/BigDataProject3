// scalastyle:off println
package org.apache.spark.examples.mllib
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint

// $example off$
object Task7_1_3 {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
      .appName("T3")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "trainLibSVM.csv")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 64

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification tree model:\n ${model.toDebugString}")

    // Save and load model
    //model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
    //val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    // $example off$


        // Compute raw scores on the test set.
        val scoreAndLabels = testData.map { point =>
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

        sc.stop()
      }


}
