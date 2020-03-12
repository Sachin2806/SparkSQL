package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ProgramSQL1 {
  
  def main(args: Array[String]){
    
     
    val conf = new SparkConf()
                  .setAppName("CombineByKey")
                  .setMaster("local")
                  
    val sc = new SparkContext(conf)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    
    val dfs = sqlcontext.read.json("C:/Users/CSC/workspace/Scala_DataFrames/Files/employee.json")
   
    //To see the data in the DataFrame  
    dfs.show()
    
    //To see the Structure (Schema) of the DataFrame
    dfs.printSchema()
    
    //To fetch only the required column
    dfs.select("name").show()
    
    //Finding the employees whose age is greater than 23 (age > 23).
    dfs.filter(dfs("age") > 23).show()
    
    //counting the number of employees who are of the same age.
    dfs.groupBy("age").count().show()
    
    dfs.select("age").distinct().show()
    
  }
}