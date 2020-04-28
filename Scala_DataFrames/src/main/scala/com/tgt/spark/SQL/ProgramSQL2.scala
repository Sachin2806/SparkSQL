package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._

//import spark.implicits._

object ProgramSQL2 {
  
  def main(args: Array[String]){
      
   val conf = new SparkConf()
                  .setAppName("ProgramJson2")
                  .setMaster("local")   
                  
    val spark = SparkSession
               .builder()
               .appName("ProgramJson2")
               .config(conf)
               .config("spark.master", "local")
               .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
               .getOrCreate()
               
    val sc = spark.sparkContext
    import spark.implicits._
    
  
    val baby_names = spark.read
                          .format("com.databricks.spark.csv")
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/Baby_Names__Beginning_2007.csv")
  
    //Register temp table from dataframe
    val baby_names_Table = baby_names.registerTempTable("names")
    
    //List all tables in Spark's catalog
    spark.catalog.listTables().show()
    
    //List catalog tables using Spark SQL
    spark.sql("show tables").show()
   
    val distinctYears = spark.sql("select distinct Year from names")
    distinctYears.collect.foreach(println)
    
    baby_names.printSchema
    val popular_names = spark.sql("select distinct(`First Name`), count(County) as cnt from names group by `First Name` order by cnt desc LIMIT 10")
    popular_names.collect.foreach(println)
  
  }
}