package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.functions._

object ProgramDF1 {
  
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
               
   //Avg, max.min, mean, sum in Data Frames
   dfQuestions.select(avg("score")).show()
   dfQuestions.select(max("score")).show()      
   dfQuestions.select(min("score")).show()
   dfQuestions.select(mean("score")).show()
   dfQuestions.select(sum("score")).show()
   
   // Group by with statistics
   dfQuestions
   .filter("id > 1 and id < 10")
   .filter("owner_userid is not null")
   .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
   .groupBy(dfQuestions.col("owner_userid"))
   .agg(avg("score"), max("answer_count"))
   .show()
  
  //DataFrame Statistics using describe() method - Descibe is a short cut to 
  //avg, max.min, mean, sum in Data Frames
  val dfQuestionsStatistics = dfQuestions.describe()
  dfQuestionsStatistics.show()
  
  // Correlation - is more advanced statistics
  val correlation = dfQuestions.stat.corr("score", "answer_count")
  println("Correlation between column score and answer_count = " + correlation)
  
   // Covariance - is more advanced statistics
  val covariance = dfQuestions.stat.cov("score", "answer_count")
  println("Covariance between column score and answer_count = " + covariance)
  
  // Frequent Items- is more advanced statistics
  val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
  dfFrequentScore.show()
  
  //Crosstab- is more advanced statistics
  val dfScoreByUserid  = dfQuestions
                         .filter("owner_userid > 0 and owner_userid < 10")
                         .stat
                         .crosstab("score", "owner_userid")                         
  dfScoreByUserid.show()
  
  // find all rows where answer_count in (1, 2, 8, 9)
  val dfQuestionsByAnswerCount = dfQuestions
                                 .filter("owner_userid > 0")
                                 .filter("owner_userid IN(1, 2, 8, 9)")
  
  // count how many rows match answer_count in (1, 2, 8, 9)
  dfQuestionsByAnswerCount
    .groupBy("answer_count")
    .count()
    .show()
  
  //Create a fraction map where we are only interested:
  //- 50% of the rows that have answer_count = 5
  //- 10% of the rows that have answer_count = 10
  //- 100% of the rows that have answer_count = 20
  //Note also that fractions should be in the range [0, 1]
  val fractionKeyMap = Map(13 -> 0.5, 5 -> 0.1, 25 -> 1.0, 58 -> 1.0, 33 -> 1.0)
  
  dfQuestionsByAnswerCount
      .stat
      .sampleBy("answer_count", fractionKeyMap, 7L)
      .groupBy("answer_count")
      .count()
      .show()

  
  }
}