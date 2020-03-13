package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits

object ProgramSQL3 {
  
  def main(args: Array[String]){
    
  val conf = new SparkConf()
                .setAppName("ProgramSQL3")
                .setMaster("local")
  
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
  val textRDD = sc.textFile("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/emp_data.csv")
  textRDD.foreach(println)
  
  val empRDD = textRDD.map
  {  line =>  val col = line.split(",")
     Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6))    
  }
  
//  val empDF = empRDD.toDF()
  
  
  }
  
  case class Employee(empno:String, 
                      ename:String, 
                      designation:String, 
                      manager:String, 
                      hire_date:String,
                      sal:String, 
                      deptno:String)
 
}