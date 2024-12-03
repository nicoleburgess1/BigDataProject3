import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Task7_2_3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("T7.2.3")
      .config("spark.master", "local")
      .getOrCreate()

    val initialdf = spark.read.csv("train.csv")
      .toDF("CustomerID", "PurchaseDate", "ProductCategory", "ProductPrice", "Quantity", "TotalPurchaseAmount", "PaymentMethod", "CustomerAge", "Returns"," CustomerName", "Age", "Gender", "Churn")
      .withColumn("ProductPrice", col("ProductPrice").cast("Double"))
      .withColumn("Quantity", col("Quantity").cast("Double"))
      .withColumn("CustomerAge", col("CustomerAge").cast("Double"))
      .withColumn("Returns", col("Returns").cast("Double"))
      .withColumn("Churn", col("Churn").cast("Double"))

    val df = initialdf.na.fill(0, Array("ProductPrice", "Quantity", "CustomerAge", "Returns", "Churn"))

    //After you stringindexing and onehotencoding for categorical features:
    val indexer = new StringIndexer().setInputCol("ProductCategory").setOutputCol("ProductCategoryIndex")
    val encoder = new OneHotEncoder().setInputCol("ProductCategoryIndex").setOutputCol("ProductCategoryEncoded")
    val indexer2 = new StringIndexer().setInputCol("Gender").setOutputCol("GenderIndex")
    val encoder2 = new OneHotEncoder().setInputCol("GenderIndex").setOutputCol("GenderEncoded")
    val indexer3 = new StringIndexer().setInputCol("PaymentMethod").setOutputCol("PaymentMethodIndex")
    val encoder3 = new OneHotEncoder().setInputCol("PaymentMethodIndex").setOutputCol("PaymentMethodEncoded")

    //You assemble features into a single vector (Combines multiple features (both encoded categorical and numerical) into a single column called features.):
    val assembler = new VectorAssembler() .setInputCols(Array("ProductPrice", "ProductCategoryEncoded", "GenderEncoded", "PaymentMethodEncoded", "Quantity", "CustomerAge", "Returns")) .setOutputCol("features")

    //Then prepare a pipeline:
    val pipeline = new Pipeline().setStages(Array(indexer, encoder, indexer2, encoder2, indexer3, encoder3, assembler))

    //Fit and transform data
    val preparedData = pipeline.fit(df).transform(df)


    //Split into training and test sets
    val Array(trainingData, testData) = preparedData.randomSplit(Array(0.8, 0.2))

    val lr = new LogisticRegression() .setLabelCol("Churn") .setFeaturesCol("features")
    val model = lr.fit(trainingData)

    //Make predictions
    val predictions = model.transform(testData)

    //Evaluate model
    val scoreAndLabels: RDD[(Double, Double)] = predictions.select("probability", "Churn")
      .rdd
      .map(row => (row.getAs[org.apache.spark.ml.linalg.Vector]("probability")(1), row.getAs[Double]("Churn")))


    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)


    val f1Scores = metrics.fMeasureByThreshold()
    f1Scores.collect().foreach { case (threshold, f1Score) =>
      println(s"Threshold: $threshold, F1 Score: $f1Score")
    }

    val auROC = metrics.areaUnderROC()
    println(s"Area under curve = $auROC")

    val auPRC = metrics.areaUnderPR()
    println(s"Area under precision-recall curve = $auPRC")


    val trainingSummary = model.summary
    val accuracy = trainingSummary.accuracy
    val falsePositiveRate = trainingSummary.weightedFalsePositiveRate
    val truePositiveRate = trainingSummary.weightedTruePositiveRate
    val fMeasure = trainingSummary.weightedFMeasure
    val precision = trainingSummary.weightedPrecision
    val recall = trainingSummary.weightedRecall
    println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
      s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall")
    



    preparedData.select("CustomerID", "ProductPrice", "Quantity", "TotalPurchaseAmount", "CustomerAge", "Age", "Churn", "ProductCategoryEncoded", "GenderEncoded", "PaymentMethodEncoded", "features").show()
  }
}
