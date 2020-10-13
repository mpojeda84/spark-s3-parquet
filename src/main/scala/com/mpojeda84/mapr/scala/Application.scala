package com.mpojeda84.mapr.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object Application {

  def main (args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)

    val spark = SparkSession.builder.appName("Simple Example").getOrCreate
    val sc = spark.sparkContext

    val conf = sc.hadoopConfiguration;

    conf.set("fs.s3a.access.key", "accessKey1")
    conf.set("fs.s3a.secret.key", "secretKey1")
    conf.set("fs.s3a.endpoint", "http://10.163.169.42:9000")
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.multiobjectdelete.enable", "true")
    conf.set("fs.s3a.impl", classOf[org.apache.hadoop.fs.s3a.S3AFileSystem].getName)


    println("### RUNNING ###")
    println(argsConfiguration.source)
    println(argsConfiguration.target)


    val s3data = sc.textFile(argsConfiguration.source)
    val total = s3data.count()
    println("Count in S3: " + total)

    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns:_*)

    df.write
      .partitionBy("language")
      .mode(SaveMode.Overwrite)
      .parquet(argsConfiguration.target)

    println("### FINISHED ###")
    spark.stop()


  }



}