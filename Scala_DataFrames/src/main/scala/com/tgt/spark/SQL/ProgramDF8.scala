package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ProgramDF8 {
  
  def main(args: Array[String]){
    
  val conf = new SparkConf()
                  .setAppName("CombineByKey")
                  .setMaster("local")
                  
    val sc = new SparkContext(conf)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)    
               
    val tagsDF1 = sqlcontext.read.json("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/tags_sample.json")
    tagsDF1.show()

  }
  
}