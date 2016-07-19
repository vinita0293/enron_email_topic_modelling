package com.gudvin.enron.utils

import scala.reflect.runtime.universe._
/**
  * Created by vinita on 7/7/16.
  */

case class Attribute(User_Name: String,
                     Topic: String,
                     File_No: String,
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

object EnronUtils {
  //getClassFieldNames(Attribute).foreach(println)
  val attributeFields = getClassFieldNames(Attribute).map(_.replaceAll("_","-")).toArray

  def EnronParser(input: (String, String)): Attribute ={
    val filePathData = input._1.split("/").takeRight(3)
    val fileContent = input._2.split("\r")

    val breakPoint = fileContent.indexWhere(_.isEmpty)
    val fieldsContent = fileContent.take(breakPoint)

    val message = fileContent.drop(breakPoint+1).mkString("\n")

    val result = Attribute(User_Name = filePathData(0),
      Topic = filePathData(1),
      File_No = filePathData(2),
      Message_ID = getValueOfKey("Message-ID",fieldsContent),
      Date = getValueOfKey("Date",fieldsContent),
      From =getValueOfKey("From",fieldsContent),
      To = getValueOfKey("To",fieldsContent),
      Subject = getValueOfKey("Subject",fieldsContent),
      Cc = getValueOfKey("Cc",fieldsContent),
      Mime_Version = getValueOfKey("Mime-Version",fieldsContent),
      Content_Type = getValueOfKey("Content-Type",fieldsContent),
      Content_Transfer_Encoding = getValueOfKey("Content-Transfer-Encoding",fieldsContent),
      Bcc = getValueOfKey("Bcc",fieldsContent),
      X_From =getValueOfKey("X-From",fieldsContent),
      X_To = getValueOfKey("X-To",fieldsContent),
      X_cc = getValueOfKey("X-cc",fieldsContent),
      X_bcc = getValueOfKey("X-bcc,fieldsContent", fieldsContent),
      X_Folder = getValueOfKey("X-Folder",fieldsContent),
      X_Origin = getValueOfKey("X-Origin",fieldsContent),
      X_FileName = getValueOfKey("X-FileName" ,fieldsContent),
      Message = message
    )

    result
  }

  def getValueOfKey(key: String, fieldsContent: Array[String]): String ={

    val requiredValueIndex = fieldsContent.indexWhere(_.contains(key+":"))
    val nextValueIndex = key match {
      case "X-FileName"=> fieldsContent.size
      case _ =>  val nextKey = fieldsContent.drop(requiredValueIndex+1).map(_.split(" ")(0)).filter(_.endsWith(":")).head
        fieldsContent.indexWhere(_.contains(nextKey))
    }

   fieldsContent.slice(requiredValueIndex,nextValueIndex)
      .mkString("\n").trim
  }

  def getClassFieldNames(cc: AnyRef) = {
    typeOf[Attribute].members.map(_.toString.split(" ")).filter(_(0).equals("value")).map(_(1)).toSeq.distinct.reverse
  }

}
