package com.tgt.spark.SQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext   


object ProgramSQL6 {
  
  def main(args: Array[String])
  {
    
  val spark = SparkSession
                .builder()
                .appName("Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
                .getOrCreate()
              
   val sc = spark.sparkContext
   import spark.implicits._
  
   val columns = Seq("language","users_count")
   val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
   val rdd = spark.sparkContext.parallelize(data)
   
    val data1 = Array(1, 2, 3, 4, 5)
    val rdd1 = sc.parallelize(data1)
    
   //Creating Data Frames Using toDF() functions
   val dfFromRDD1 = columns.toDF()
   val dfFromRDD2 = data.toDF("language","users_count")
   val dataFrame = rdd.toDF()
   
   dataFrame.show()
   
   dfFromRDD1.show()
   dfFromRDD2.show()
   
   //Using Spark createDataFrame() from SparkSession
   val dfFromRDD3 = spark.createDataFrame(data).toDF(columns:_*)
   dfFromRDD3.show()
   
   val values = List(1,2,3,4,5)
   val df = values.toDF()
   df.show()
   
  }
  
}