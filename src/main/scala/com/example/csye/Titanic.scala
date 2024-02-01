package com.example.csye

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Titanic {
  def main(args: Array[String]): Unit = {
    // Initialize a Spark session
    // Set the master URL in the Spark configuration
    val conf = new SparkConf().setAppName("TitanicAnalysis").setMaster("local")
    val sc = new SparkContext(conf)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("TitanicAnalysis")
      .getOrCreate()

    // Load the Titanic dataset
    val datasetPath = "train.csv"
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(datasetPath)

    // Question 1: Average ticket fare for each Ticket class
    val averageFareByClass = df.groupBy("Pclass").avg("Fare").orderBy("Pclass")
    averageFareByClass.show()

    // Question 2: Survival percentage for each Ticket class
    val survivalPercentageByClass = df.groupBy("Pclass")
      .agg((sum(when(col("Survived") === 1, 1).otherwise(0)) / count("*")).alias("SurvivalPercentage")).orderBy(desc("SurvivalPercentage"))
    val highestSurvivalClass = survivalPercentageByClass.select("Pclass").first()(0).toString
    survivalPercentageByClass.show()
    println(s"$highestSurvivalClass class has the highest survival rate.")

    // Question 3: Find the number of passengers who could possibly be Rose
    val possibleRoseCount = df.filter(col("Sex") === "female" && col("Pclass") === 1 && col("Age") === 17 && col("SibSp") === 0 && col("Parch") === 1).count()
    df.filter(col("Sex") === "female" && col("Pclass") === 1 && col("Age") === 17 && col("SibSp") === 0 && col("Parch") === 1).show()
    println(s"Number of passengers who could possibly be Rose: $possibleRoseCount")

    // Question 4: Find the number of passengers who could possibly be Jack
    val possibleJackCount = df.filter(col("Sex") === "male" && col("Pclass") === 3 && (col("Age") === 19 || col("Age") === 20) && col("SibSp") === 0).count()
    df.filter(col("Sex") === "male" && col("Pclass") === 3 && (col("Age") === 19 || col("Age") === 20) && col("SibSp") === 0).show(30)
    println(s"Number of passengers who could possibly be Jack: $possibleJackCount")

    // Question 5: Relation between ages and ticket fare, and age group survival
    val ageGroupUDF = udf((age: Double) => {
      if (age >= 1 && age <= 10) "1-10"
      else if (age >= 11 && age <= 20) "11-20"
      else if (age >= 21 && age <= 30) "21-30"
      else if (age >= 31 && age <= 40) "31-40"
      else if (age >= 41 && age <= 50) "41-50"
      else if (age >= 51 && age <= 60) "51-60"
      else if (age >= 61 && age <= 70) "61-70"
      else if (age >= 71 && age <= 80) "71-80"
      else "81+"
    })

    val dfWithAgeGroups = df.withColumn("AgeGroup", ageGroupUDF(col("Age")))

    // Calculate average fare per age group
    val averageFareByAgeGroup = dfWithAgeGroups.groupBy("AgeGroup").avg("Fare").sort("AgeGroup")
    averageFareByAgeGroup.show()

    // Calculate survival rate per age group
    val survivalRateByAgeGroup = dfWithAgeGroups.groupBy("AgeGroup")
      .agg((sum(when(col("Survived") === 1, 1).otherwise(0)) / count("*")).alias("SurvivalRate"))
      .sort("AgeGroup")
    val highestSurvivalAgeGroup = survivalRateByAgeGroup.orderBy(desc("SurvivalRate"))
      .select("AgeGroup").first()(0).toString
    survivalRateByAgeGroup.show()
    println(s"age in $highestSurvivalAgeGroup has the highest survival rate.")


    // Stop the Spark session
    sc.stop()
    spark.stop()
  }
}