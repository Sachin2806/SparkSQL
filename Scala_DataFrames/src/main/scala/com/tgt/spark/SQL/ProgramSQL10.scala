package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd._

object ProgramSQL10 {
  
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
               
  dfTags.createOrReplaceTempView("so_tags")
  
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

  // filter dataframe
  val dfQuestionsSubset  = dfQuestions.filter("score > 400 and score < 480").toDF()
  
  // register temp table
  dfQuestionsSubset.createOrReplaceTempView("so_questions")
                    
  // SQL Inner Join
  spark.sql("select t.*, q.* from so_questions q inner join so_tags t on t.id = q.id").show()
                    
  // SQL Left Outer Join
  spark.sql("select t.*, q.* from so_questions q left outer join so_tags t on t.id = q.id").show()
  
  // SQL Right Outer Join
  spark.sql("select t.*, q.* from so_questions q right outer join so_tags t on t.id = q.id").show()
                    
  // SQL Distinct
  spark.sql("select distinct id from so_tags").show()
                                     
  }  
}