package edu.gatech.cse6250.main

import edu.gatech.cse6250.clustering.Metrics
import edu.gatech.cse6250.features.FeatureConstruction
import edu.gatech.cse6250.helper.{ CSVHelper, SparkHelper }
import edu.gatech.cse6250.model.{ Diagnostic, LabResult, Medication }
import edu.gatech.cse6250.phenotyping.T2dmPhenotype
import org.apache.spark.mllib.clustering.{ GaussianMixture, KMeans, StreamingKMeans }
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{ DenseMatrix, Matrices, Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat
import java.sql.Date

import scala.io.Source

object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.{ Level, Logger }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkHelper.spark
    val sc = spark.sparkContext
    //  val sqlContext = spark.sqlContext

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(spark)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication)
    )

    // =========== USED FOR AUTO GRADING CLUSTERING GRADING =============
    // phenotypeLabel.map{ case(a,b) => s"$a\t$b" }.saveAsTextFile("data/phenotypeLabel")
    // featureTuples.map{ case((a,b),c) => s"$a\t$b\t$c" }.saveAsTextFile("data/featureTuples")
    // return
    // ==================================================================

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val (kMeansPurity, gaussianMixturePurity, streamingPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] purity of StreamingKmeans is: $streamingPurity%.5f")

    /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
    )

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)

    val (kMeansPurity2, gaussianMixturePurity2, streamingPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] purity of StreamingKmeans is: $streamingPurity2%.5f")
  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures: RDD[(String, Vector)]): (Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    println("phenotypeLabel: " + phenotypeLabel.count)
    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray))) })
    println("features: " + features.count)
    val rawFeatureVectors = features.map(_._2).cache()
    val rawFeatureIDs = features.map(_._1)
    println("rawFeatureVectors: " + rawFeatureVectors.count)

    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]

    def transform(feature: Vector): Vector = {
      val scaled = scaler.transform(Vectors.dense(feature.toArray))
      Vectors.dense(Matrices.dense(1, scaled.size, scaled.toArray).multiply(densePc).toArray)
    }

    /**
     * TODO: K Means Clustering using spark mllib
     * Train a k means model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.purity
     * Remove the placeholder below after your implementation
     */
    val labels = phenotypeLabel
      .map(_._2)
      .zipWithIndex
      .map(x => (x._2, x._1))
      .cache()

    val numClusters = 3
    val iterations = 20
    featureVectors.cache()
    val kMeans = new KMeans()
      .setK(numClusters)
      .setMaxIterations(iterations)
      .setInitializationMode("k-means||")
      .setInitializationSteps(1)
      .setSeed(6250L)
    val kMeansModel = kMeans.run(featureVectors)
    val kMeansCluster = kMeansModel.predict(featureVectors)
    val kMeansResult = rawFeatureIDs.zip(kMeansCluster)
    val compareKMeans = kMeansResult.join(phenotypeLabel).map(_._2)
    val kMeansPurity = Metrics.purity(compareKMeans)
    //val kMeansPurity = 0.0

    /**
     * TODO: GMMM Clustering using spark mllib
     * Train a Gaussian Mixture model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.purity
     * Remove the placeholder below after your implementation
     */
    val GMMClusters = new GaussianMixture().setK(numClusters).setMaxIterations(iterations).setSeed(6250L).run(featureVectors).predict(featureVectors)
    val GMMResult = rawFeatureIDs.zip(GMMClusters)
    val compareGMM = GMMResult.join(phenotypeLabel).map(_._2)
    val gaussianMixturePurity = Metrics.purity(compareGMM)
    //val gaussianMixturePurity = 0.0

    /**
     * TODO: StreamingKMeans Clustering using spark mllib
     * Train a StreamingKMeans model using the variabe featureVectors as input
     * Set the number of cluster K = 3, DecayFactor = 1.0, number of dimensions = 10, weight for each center = 0.5, seed as 6250L
     * In order to feed RDD[Vector] please use latestModel, see more info: https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.mllib.clustering.StreamingKMeans
     * To run your model, set time unit as 'points'
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.purity
     * Remove the placeholder below after your implementation
     */
    val streamKmeans = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(10, 0.5, 6250L)
    val streamKmeansModel = streamKmeans.latestModel()
    val streamKmeansPred = streamKmeansModel.update(featureVectors, 1.0, "batches").predict(featureVectors)
    val streamingKeansTest = features.map(_._1).zip(streamKmeansPred).join(phenotypeLabel).map(_._2)
    val streamKmeansPurity = Metrics.purity(streamingKeansTest)

    //val streamKmeansPurity = 0.0
    (kMeansPurity, gaussianMixturePurity, streamKmeansPurity)
  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
   *
   * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def sqlDateParser(input: String, pattern: String = "yyyy-MM-dd'T'HH:mm:ssX"): java.sql.Date = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    new java.sql.Date(dateFormat.parse(input).getTime)
  }

  def loadRddRawData(spark: SparkSession): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /* the sql queries in spark required to import sparkSession.implicits._ */
    import spark.implicits._
    val sqlContext = spark.sqlContext

    /* a helper function sqlDateParser may useful here */

    /**
     * load data using Spark SQL into three RDDs and return them
     * Hint:
     * You can utilize edu.gatech.cse6250.helper.CSVHelper
     * through your sparkSession.
     *
     * This guide may helps: https://bit.ly/2xnrVnA
     *
     * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
     * Be careful when you deal with String and numbers in String type.
     * Ignore lab results with missing (empty or NaN) values when these are read in.
     * For dates, use Date_Resulted for labResults and Order_Date for medication.
     *
     */

    /**
     * TODO: implement your own code here and remove
     * existing placeholder code below
     */
    val format = "yyyy-MM-dd'T'HH:mm:ssX"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    val lab_results = CSVHelper.loadCSVAsTable(spark, "data/lab_results_INPUT.csv": String, "LAB")
    val medication_data = CSVHelper.loadCSVAsTable(spark, "data/medication_orders_INPUT.csv": String, "MED")
    val patient_IDdata = CSVHelper.loadCSVAsTable(spark, "data/encounter_INPUT.csv": String, "DIAG")
    val patient_diagnostic = CSVHelper.loadCSVAsTable(spark, "data/encounter_dx_INPUT.csv": String, "DIAGDX")
    val lab = sqlContext.sql("SELECT Member_ID as patientID, Date_Collected as date,Result_Name as testName,Numeric_Result as value FROM LAB WHERE WHERE Numeric_Result!=''")
    val lab_rdd: RDD[LabResult] = lab.rdd.map { r: Row => new LabResult(r.getString(0), this.sqlDateParser(r.getString(1)), r.getString(2).toLowerCase, r.getString(3).filterNot(",".toSet).toDouble) }
    val med = sqlContext.sql("SELECT Member_ID as patientID, Order_Date as date,Drug_Name as medicine FROM MED")
    val med_rdd: RDD[Medication] = med.rdd.map { r: Row => new Medication(r.getString(0), this.sqlDateParser(r.getString(1)), r.getString(2).toLowerCase) }
    val diag = sqlContext.sql("SELECT Member_ID as patientID, Encounter_DateTime as date,DIAGDX.code as code FROM DIAG JOIN DIAGDX ON DIAG.Encounter_ID=DIAGDX.Encounter_ID")
    val diag_rdd: RDD[Diagnostic] = diag.rdd.map { r: Row => new Diagnostic(r.getString(0), this.sqlDateParser(r.getString(1)), r.getString(2).toLowerCase) }
    val medication: RDD[Medication] = med_rdd
    val labResult: RDD[LabResult] = lab_rdd
    val diagnostic: RDD[Diagnostic] = diag_rdd

    (medication, labResult, diagnostic)
  }

}
