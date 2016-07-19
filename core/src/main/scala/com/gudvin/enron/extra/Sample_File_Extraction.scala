package com.gudvin.enron.extra

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 7/15/16.
  */
case class Fields(
                     Message_ID: String,
                     Date: String,
                     From: String,
                     To: String,
                     Subject: String,
                     Cc: String,
                     Mime_Version: String,
                     Content_Type: String,
                     Content_Transfer_Encoding: String,
                     Bcc: String,
                     X_From: String,
                     X_To: String,
                     X_cc: String,
                     X_bcc: String,
                     X_Folder: String,
                     X_Origin: String,
                     X_FileName: String,
                     Message: String
                    )

object Sample_File_Extraction {


  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "spark://vinita-Lenovo-G50-80:7077"

    val conf: SparkConf = new SparkConf()
      .setAppName("Enron starting")
      .setMaster("local")
      .setSparkHome(sparkHome)

    val sc=new SparkContext(conf)

     val loadedfile:RDD[String]=sc.textFile("/home/vinita/Downloads/enron_example/allen-p/all_documents/1.");

    val content=loadedfile.toString().split("\r")

    val break=content.indexWhere(_.isEmpty)
    val fields=content.take(break+1)

    val message = content.drop(break+1).toString()

    val result = Fields(Message_ID = getValueOfKey("Message-ID",fields),
      Date = getValueOfKey("Date",fields),
      From =getValueOfKey("From",fields),
      To = getValueOfKey("To",fields),
      Subject = getValueOfKey("Subject",fields),
      Cc = getValueOfKey("Cc",fields),
      Mime_Version = getValueOfKey("Mime-Version",fields),
      Content_Type = getValueOfKey("Content-Type",fields),
      Content_Transfer_Encoding = getValueOfKey("Content-Transfer-Encoding",fields),
      Bcc = getValueOfKey("Bcc",fields),
      X_From =getValueOfKey("X-From",fields),
      X_To = getValueOfKey("X-To",fields),
      X_cc = getValueOfKey("X-cc",fields),
      X_bcc = getValueOfKey("X-bcc,fieldsContent", fields),
      X_Folder = getValueOfKey("X-Folder",fields),
      X_Origin = getValueOfKey("X-Origin",fields),
      X_FileName = getValueOfKey("X-FileName" ,fields),
      Message = message
    )


  }

  def getValueOfKey(field:String,fieldcontent:Array[String]): String =
  {

""
  }
  }
