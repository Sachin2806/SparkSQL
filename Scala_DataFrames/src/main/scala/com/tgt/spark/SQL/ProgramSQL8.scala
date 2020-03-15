package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd._

object ProgramSQL8 {
  
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
               
  dfQuestionsCSV.printSchema()
  
  //After Type-Casting
  
  val dfQuestions = dfQuestionsCSV.select(
                    dfQuestionsCSV.col("id").cast("integer"),
                    dfQuestionsCSV.col("creation_date").cast("timestamp"),
                    dfQuestionsCSV.col("closed_date").cast("integer"),
                    dfQuestionsCSV.col("deletion_date").cast("date"),
                    dfQuestionsCSV.col("score").cast("integer"),
                    dfQuestionsCSV.col("owner_userid").cast("integer"),
                    dfQuestionsCSV.col("answer_count").cast("integer"))
  
  dfQuestions.printSchema()
  dfQuestions.show()
  
  //Operate on a sliced dataframe
  val dfQuestionsSubset  = dfQuestions.filter("score > 400 and score < 480").toDF()
  dfQuestionsSubset.show()
  
  // DataFrame Query: Join
  dfQuestionsSubset.join(dfTags, "id").show()
  
  //DataFrame Query: Join and select columns
  dfQuestionsSubset.join(dfTags, "id").select("id", "creation_date", "score", "tag").show()
  
  //DataFrame Query: Join on explicit columns
  dfQuestionsSubset.join(dfTags, dfTags("id") === dfQuestionsSubset("id")).show()
  
  // DataFrame Query: Inner Join
  dfQuestionsSubset.join(dfTags, Seq("id"), "inner").show()
  
  // DataFrame Query: Left Outer Join
  dfQuestionsSubset.join(dfTags, Seq("id"), "left_outer").show()
  
  // DataFrame Query: Right Outer Join
   dfQuestionsSubset.join(dfTags, Seq("id"), "right_outer").show()
   
  // DataFrame Query: Distinct
   dfTags.select("id").distinct().show()
  
  }
  
}