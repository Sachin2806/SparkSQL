package com.tgt.spark.SQL

import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

object ProgramDF5 {
  
  def main(args: Array[String]){
    
  val spark = SparkSession
              .builder()
              .appName("ProgramDF5")
              .config("spark.master", "local")
              .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
              .getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  //Demo of withColumns in Data Frames
  
  val got = Seq((1,"Bran"),(2,"Jon"))
  val gotDF = got.toDF("id", "name")
  gotDF.show()
  
  //Different ways of selecting columns in a DF
  //  gotDF.select(col("id")).show()
  //  gotDF.select(column("id"))
  //  gotDF.select($"id")
  //  gotDF.select('id)
  //  gotDF.select('id).show
  val teenage = (15 to 25)
  val gotData = Seq((101,"Bran",10,"Stark"),(221,"Jon",16,null),(11,"Ned",50,"Stark"),(21,"Tyrion",40,"Lanister"))
  val gotDataDF = gotData.toDF("id","name","age","house")
  gotDataDF.show()
  
  //Getting the current age of the GOT stars and add 8 years to each of character’s age and 
  //have a new column with the current age
  gotDataDF.withColumn("char_current_age", col("age").+(8)).show()
  gotDataDF.withColumn("older_than_brnch_age", col("age").-(10)).show()
  gotDataDF.select(gotDataDF("age") + gotDataDF("id")).show()
  
  gotDataDF.withColumn("multiply_10", col("age") *  10).show()
  gotDataDF.withColumn("multiply_10", col("age") multiply 10).show()
  gotDataDF.withColumn("divide_10", col("age") / 10).show()
  gotDataDF.withColumn("divide_10", col("age") divide 10).show()
  gotDataDF.withColumn("mod_10", col("age") mod  10).show()
  gotDataDF.withColumn("mod_10", col("age") %  10).show()
  
  gotDataDF.withColumn("teenage", col("age").isin(teenage:_*)).show()
  gotDataDF.withColumn("Starts with name B", col("name").like("B%")).show()
  gotDataDF.withColumn("has O ",col("name").rlike("^(Bran|Jon)")) .show()
  
  gotDataDF.withColumn("has O ",!col("name").like("%o%")).show()
  gotDataDF.withColumn("has O ",!col("name").rlike("^(Bran|Jon)")).show()
  
  gotDataDF.withColumn("house_unknown",col("house").isNull).show()
  
  }
  
}