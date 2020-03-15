package com.tgt.spark.SQL

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ProgramDF4 {
  
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
   
  val seqTags = Seq(1 -> "so_java", 1 -> "so_jsp", 2 -> "so_erlang", 3 -> "so_scala", 3 -> "so_akka" )
  
  //Create DataFrame from collection
  val dfMoreTags = seqTags.toDF("id", "tag")              
  dfMoreTags.show()             
  
  //DataFrame Union
  val dfUnionOfTags = dfTags.union(dfMoreTags).filter("id in (1,11,13)").orderBy("id")
  dfUnionOfTags.show()
  
  //DataFrame Intersection
  val dfIntersectionTags = dfTags.intersect(dfUnionOfTags)
  dfIntersectionTags.show()
  
  //Append column to DataFrame using withColumn()
  val dfSplitColumn = dfMoreTags
                      .withColumn("tmp", split($"tag", "_"))
                      .select($"id",
                              $"tag",
                              $"tmp".getItem(0).as("so_prefix"),
                              $"tmp".getItem(1).as("so_tag")).drop("tmp")
                              
  dfSplitColumn.show(10)
  }
  
}