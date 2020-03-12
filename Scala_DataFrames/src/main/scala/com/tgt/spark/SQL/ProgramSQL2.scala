package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._

//import spark.implicits._

object ProgramSQL2 {
  
  def main(args: Array[String]){
    
  val conf = new SparkConf()
                  .setAppName("CombineByKey")
                  .setMaster("local")
                  
  val sc = new SparkContext(conf)
  val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
  
  val baby_names = sqlcontext
                   .read
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("C:/Users/CSC/workspace/Scala_DataFrames/Files/Baby_Names.csv")
  
  baby_names.take(5).foreach(println)
  val baby_names_Table = baby_names.registerTempTable("names")
  val distinctYears = sqlcontext.sql("select distinct Year from names")
  distinctYears.collect.foreach(println)
  baby_names.printSchema
  val popular_names = sqlcontext.sql("select distinct(`First Name`), count(County) as cnt from names group by `First Name` order by cnt desc LIMIT 10")
  
  
  }
}