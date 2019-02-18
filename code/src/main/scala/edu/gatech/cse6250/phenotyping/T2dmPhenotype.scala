package edu.gatech.cse6250.phenotyping

import edu.gatech.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.rdd.RDD

object T2dmPhenotype {

  /** Hard code the criteria */
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
    "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
    "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
    "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
    "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
    "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
    "avandia", "actos", "actos", "glipizide")

  /**
   * Transform given data set to a RDD of patients and corresponding phenotype
   *
   * @param medication medication RDD
   * @param labResult  lab result RDD
   * @param diagnostic diagnostic code RDD
   * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
   */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
     * Remove the place holder and implement your code here.
     * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
     * When testing your code, we expect your function to have no side effect,
     * i.e. do NOT read from file or write file
     *
     * You don't need to follow the example placeholder code below exactly, but do have the same return type.
     *
     * Hint: Consider case sensitivity when doing string comparisons.
     */

    val sc = medication.sparkContext

    /** Hard code the criteria */
    // val type1_dm_dx = Set("code1", "250.03")
    // val type1_dm_med = Set("med1", "insulin nph")
    // use the given criteria above like T1DM_DX, T2DM_DX, T1DM_MED, T2DM_MED and hard code DM_RELATED_DX criteria as well
    val type1_dm_dx = Set("code1","250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43", "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")
    val type2_dm_dx = Set("code2","250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6", "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")
    val type1_dm_med = Set("med1", "lantus", " insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente","insulin,ultralente")
    val type2_dm_med = Set("med2", "chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")


    /** Find CASE Patients */
    val type1_dm_diag = diagnostic.filter{a => type1_dm_dx(a.code)}.map(_,patientID)
    val type2_dm_diag = diagnostic.filter{a => type2_dm_dx(a.code)}.map(_,patientID)
    val type1_dm_medi = diagnostic.filter{a => type1_dm_med(a.medicine)}.map(_,patientID)
    val type2_dm_medi = diagnostic.filter{a => type2_dm_med(a.medicine)}.map(_,patientID)
    val totalpatients = sc.union(diagnostic.map(_,patientID),medication.map(_,patientID),labResult.map(_,patientID))
    val step1_case1 = totalpatients.subtract(type1_dm_diag).intersection(type2_dm_diag)
    val step2_case1 = step1_case1.intersection(type1_dm_medi)
    val case1 = step1_case1.subtract(step2_case1)
    val step1_case2 = step2_case1.intersection(type2_dm_medi)
    val case2 = step2_case1.subtract(step1_case2)
    val date1 = medication.filter( a => type1_dm_med(a.medicine)).map(a=> (a.patientID,a.date.getTime)).reduceByKey((x,y) => math.min(x,y))
    val date2 = medication.filter( a => type2_dm_med(a.medicine)).map(a=> (a.patientID,a.date.getTime)).reduceByKey((x,y) => math.min(x,y))
    val step1_case3 = date1.join(date2)
    val step2_case3 = step1_case3.filter(a => a._2._1 >= a._2._2).map(_._1)
    val case3 = step1_case2.intersection(step2_case3)
    val casePatients = sc.union(case1,case2,case3).distinct()
    //val casePatients = sc.parallelize(Seq(("casePatient-one", 1), ("casePatient-two", 1), ("casePatient-three", 1)))

    /** Find CONTROL Patients */
    //val controlPatients = sc.parallelize(Seq(("controlPatients-one", 2), ("controlPatients-two", 2), ("controlPatients-three", 2)))
    val glu_patients = labResult.filter{a => a.testName.contains("glucose")}.map(_.patientID).distinct()
    val abnormal_patients = labResult.filter{ a =>
      val name = a.testName
      val value = a.value
      ((name=="hba1c" && value>=6.0) ||  (name=="hemoglobin a1c" && value>=6.0) || (name=="fasting glucose" && value>=110) || (name=="fasting blood glucose" && value>=110)
        || (name=="fasting plasma glucose" && value>=110) || (name=="glucose" && value>110) || (name=="glucose, serum" && value>110))
    }.map{_.patientID}
    val unabnormal_patients = glu_patients.subtract(abnormal_patients)
    val mellitus = Set("790.21", "790.22", "790.2", "790.29", "648.81", "648.82", "648.83", "648.84", "648", "648", "648.01", "648.02", "648.03", "648.04", "791.5", "277.7", "V77.1", "256.4")
    val mellitusPatients=diagnostic.filter{ a => (mellitus(a.code) || a.code.take(3) == "250") }.map(_.patientID)
    val controlPatients=totalpatients.intersection(glu_patients).subtract(mellitusPatients).distinct()

    /** Find OTHER Patients */
    //val others = sc.parallelize(Seq(("others-one", 3), ("others-two", 3), ("others-three", 3)))
    val others = totalpatients.subtract(casePatients).subtract(controlPatients).distinct()

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    //val phenotypeLabel = sc.union(casePatients, controlPatients, others)
    val phenotypeLabel = sc.union(casePatients.map(a=> (a,1)), controlPatients.map(a=> (a,2)), others.map(a=> (a,3)))

    /** Return */
    phenotypeLabel
  }
}