package edu.gatech.cse6250.features

import edu.gatech.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   *
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val diagnosis_feature = diagnostic.map(x => ((x.patientID, x.code), 1.0)).reduceByKey(_ + _)
    diagnosis_feature
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   *
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val medication_feature = medication.map(x => ((x.patientID, x.medicine), 1.0)).reduceByKey(_ + _)
    medication_feature
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   *
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lab_sum = labResult.map(x => ((x.patientID, x.testName), x.value)).reduceByKey(_ + _)
    val lab_count = labResult.map(x => ((x.patientID, x.testName), 1.0)).reduceByKey(_ + _)
    val lab_result_feature = lab_sum.join(lab_count).map(x => (x._1, x._2._1 / x._2._2))
    lab_result_feature
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   *
   * @param diagnostic   RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val diagnosis_feature = diagnostic.map(x => ((x.patientID, x.code), 1.0)).reduceByKey(_ + _)
    val diagnosis_type = diagnosis_feature.filter(x => (candiateCode.contains(x._1._2)))
    diagnosis_type
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   *
   * @param medication          RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val medication_feature = medication.map(x => ((x.patientID, x.medicine), 1.0)).reduceByKey(_ + _)
    val medication_type = medication_feature.filter(x => (candidateMedication.contains(x._1._2)))
    medication_type

  }

  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   *
   * @param labResult    RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lab_sum = labResult.map(x => ((x.patientID, x.testName), x.value)).reduceByKey(_ + _)
    val lab_count = labResult.map(x => ((x.patientID, x.testName), 1.0)).reduceByKey(_ + _)
    val lab_result_feature = lab_sum.join(lab_count).map(x => (x._1, x._2._1 / x._2._2))
    val lab_result_type = lab_result_feature.filter(x => (candidateLab.contains(x._1._2)))
    lab_result_type
  }

  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   *
   * @param sc      SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map */

    /** transform input feature */

    /**
     * Functions maybe helpful:
     * collect
     * groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    /**
     * val result = sc.parallelize(Seq(
     * ("Patient-NO-1", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-2", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-3", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-4", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-5", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-6", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-7", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-8", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-9", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
     * ("Patient-NO-10", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))))
     */

    feature.cache()

    /** create a feature name to id map*/
    val features_index = feature.map(_._1._2).distinct().collect.zipWithIndex.toMap
    val scFeatureMap = sc.broadcast(features_index)

    /** transform input feature */
    val patientAndFeatures = feature.map(x => (x._1._1, (x._1._2, x._2))).groupByKey()
    val result = patientAndFeatures.map {
      case (target, features) =>
        val num_features = scFeatureMap.value.size
        val idx_features = features.toList.map { case (feature_name, featureValue) => (scFeatureMap.value(feature_name), featureValue) }
        val featureVector = Vectors.sparse(num_features, idx_features)
        val label_point = (target, featureVector)
        label_point
    }
    result

    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */

  }
}

