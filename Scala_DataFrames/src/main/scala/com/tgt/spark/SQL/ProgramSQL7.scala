package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits

object ProgramSQL7 {
  
  case class Employee(empno:String, ename:String, designation:String, manager:String, 
                      hire_date:String, sal:String, deptno:String)
 
  def main(args: Array[String]){
    
  val spark = SparkSession
               .builder()
               .appName("Spark SQL basic example")
               .config("spark.master", "local")
               .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
               .getOrCreate()
              
   val sc = spark.sparkContext
   import spark.implicits._
  
   val textRDD = sc.textFile("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/emp_data.csv")
  
   val empRDD = textRDD.map
   {  line =>  val col = line.split(",")
      Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6))    
   }
  
   val empDF = empRDD.toDF()
   empDF.show()
  }
  
}