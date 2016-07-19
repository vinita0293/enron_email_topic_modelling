package com.gudvin.enron.extra

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 7/5/16.
  */
case class Attribute1(Message_Id: String,Date: String,From: String,To: String,Subject: String,Mime_Version: String,
                  Content_Type: String,Content_Transfer_Encoding: String,X_From: String,X_To: String,X_CC: String,
                  X_bCC: String,X_Folder: String,X_Origin: String,X_File_Name: String)
/*
a: vale
as
as
dasda
ad
b:
 */

object Wrangler {

  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "spark://vinita-Lenovo-G50-80:7077"

    val conf: SparkConf = new SparkConf()
      .setAppName("Enron starting")
      .setMaster("local")
      .setSparkHome(sparkHome)

    val sc = new SparkContext(conf)
    val loadedFileRDD: RDD[String] = sc.textFile("/home/vinita/Downloads/maildir")

    val sqlContext = new SQLContext(sc)//
    val a= "a b c d e\n X-col:"



    //val df = loadedFileRDD.collect().mkString("\n").map(x => (x.split("X-Filename:")(1).split("\n")(0)))
    //df.persist()
    //df.registerTempTable("vini_table")






  }

}
