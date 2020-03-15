package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.functions._

object ProgramDF2 {
  
  def main(args: Array[String]){
    
  val spark = SparkSession
              .builder()
              .appName("ProgramSQL8")
              .config("spark.master", "local")
              .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
              .getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  // Create a DataFrame from reading a CSV file
  val dfTags = spark
               .read
               .option("header","true")
               .format("com.databricks.spark.csv")
               .option("inferSchema", "true")
               .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/question_tags_10K.csv")
               .toDF("id", "tag")
               
  val dfQuestionsCSV  = spark
                        .read
                        .option("header","true")
                        .format("com.databricks.spark.csv")
                        .option("inferSchema", "true")
                        .option("dateFormat","yyyy-MM-dd HH:mm:ss")
                        .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/questions_10K.csv")
                        .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")
               
  //After Type-Casting  
  val dfQuestions = dfQuestionsCSV.select(
                    dfQuestionsCSV.col("id").cast("integer"),
                    dfQuestionsCSV.col("creation_date").cast("timestamp"),
                    dfQuestionsCSV.col("closed_date").cast("integer"),
                    dfQuestionsCSV.col("deletion_date").cast("date"),
                    dfQuestionsCSV.col("score").cast("integer"),
                    dfQuestionsCSV.col("owner_userid").cast("integer"),
                    dfQuestionsCSV.col("answer_count").cast("integer"))
               
  //Approximate Quantile
  val quantiles = dfQuestions.stat.approxQuantile("score", Array(0, 0.5, 1), 0.25)
  println(" Qauntiles segments = " + quantiles.toSeq)
  
  // Bloom Filter
  val tagsBloomFilter = dfTags.stat.bloomFilter("tag", 1000L, 0.1)
  
  println("bloom filter contains java tag = " + tagsBloomFilter.mightContain("Java"))
  println("bloom filter contains some unknown tag = " + tagsBloomFilter.mightContain("unknown tag"))
  
  // Count Min Sketch
  val cmsTag = dfTags.stat.countMinSketch("tag", 0.1, 0.9, 37)
  val estimatedFrequency = cmsTag.estimateCount("Java")
  println("Estimated frequency for tag Java = " + estimatedFrequency)
  
  // Sampling With Replacement
  val dfTagsSample = dfTags.sample(true, 0.2, 37L)
  println("Number of rows in sample dfTagsSample = " + dfTagsSample.count())
  println("Number of rows in dfTags = " + dfTags.count())
  
  }
}