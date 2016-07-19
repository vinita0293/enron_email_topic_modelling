
package com.gudvin.enron

import com.gudvin.enron.utils.EnronUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 7/17/16.
  */
object Analyser {

  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "spark://vinita-Lenovo-G50-80:7077"

    val conf: SparkConf = new SparkConf()
      .setAppName("Enron starting")
      .setMaster("local[4]")
      .setSparkHome(sparkHome)



    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    //val loadedFileRDD: RDD[(String, String)] =
    //  sc.wholeTextFiles("/home/vinita/Downloads/maildir/*/{_sent_mail/,sent}*", 12)

    val loadedFileRDD: RDD[(String, String)] =
      sc.wholeTextFiles("/home/vinita/Downloads/enron_example", 12)

    val newRDD = loadedFileRDD.map(x => (x._1, x._2.replace("\n", "")))
    val resultantRDD = newRDD.map(EnronUtils.EnronParser)
    resultantRDD.cache()
    /* val userEmailIdDataArray = resultantRDD.map(records => {
       (records.User_Name,records.From)
     }).groupByKey().map(x=> (x._1,x._2.toSeq.distinct))*/
val k=resultantRDD.collect().toSeq
    /**
      * Use case 1
      * Find top 5 persons to whom a particular person has sent mails.
      */

    /**
      * RDD((un1,e1),(un1,e2),(un1,e2),(un2,e3),(u3,e4))
      * RDD((un1,Iterator(e1,e2,e2),(un2,Iterator(e3))...))
      * RDD((un1,Seq(e1,e2),(un2,Seq(e3))...))
      *
      */

    /*val toEmailIdRDD = resultantRDD.map(records => {
      records.To.split(",").map(_.trim).zip(records.X_To.split(",").map(_.trim)).toSeq
    }).flatMap(identity).groupByKey().map(x=> (x._1,x._2.toSeq.distinct))
*/

    val userEmailIdDataArray = sc.textFile("/media/vinita/Projects/study_related/Data/enron/result/contact_list")
      .map(x => { val array = x.drop(1).dropRight(2).split(",")
        array(1) = " " + array(1).substring(5)
        (array.head, array.tail.map(_.substring(7)))
      }).collect()

    val broadcastedUserEmailIdDataArray = sc.broadcast(userEmailIdDataArray)

    /**
      * (AP,(e1,e2,e3))
      * (AP,Seq(e1,e2,e3))
      * (Seq((AP,e1),(AP,e2),(AP,e3)))
      * ((AP,e1),(AP,e2),(AP,e3))
      */

    val sentEmailIdDataRDD = resultantRDD.map(records => {
      records.To.replaceAll(" \n", "").split(",").map(_.trim)
        .map(emailId => (records.User_Name, emailId))
    }.toSeq).flatMap(identity)
    /**
      * (a,List(ae1,ae2))
      * (b,List(be1,be2))
      *
      * be2
      *
      * filter
      * (b,List(be1,be2))
      *
      * head
      * (b,List(be1,be2))
      *
      * ._1
      * b
      */
    val result = sentEmailIdDataRDD.map(record => {
     (record._1,(broadcastedUserEmailIdDataArray.value.filter(_._2.contains(record._2)).map(_._1).applyOrElse(0,"U").toString,1))
      //(record._1,(record._2,1))
    })

   /* val notFound = sentEmailIdDataRDD.filter(record =>
      !broadcastedUserEmailIdDataArray.value.map(_._2.toSeq).flatMap(identity).contains(record._2))
        .distinct()

    notFound.coalesce(1).saveAsTextFile("/home/vinita/Downloads/not-found")*/
    /**
      * (AP,(RM,1))
      * (AP,(RM,1))
      * (AP,(PA,1))
      * (AP,(PA,1))
      * (AP,(PA,1))
      * (TK,(PA,1))
      * (TK,(PA,1))
      * (TK,(PA,1))
      *
      * //1
      * (AP,Iterator((RM,1),(RM,1),(RM,1),(PA,1),(PA,1)))
      * (TK,Iterator((PA,1),(PA,1),(PA,1)))
      *
      * //2
      * (AP, Map((RM,Iterator((RM,1),(RM,1)),(PA,Iterator((PA,1),(PA,1),(PA,1)))))
      *
      * //3
      * (AP, Map((RM,Iterator((1),(1)),(PA,Iterator((1),(1),(1)))))
      *
      * //4
      * (AP, Map((RM,2),(PA,3)))
      * (TK, Map((PA,3)))
      *
      */

    val result1 = result.groupByKey() //1
      .map(x => (x._1, x._2.groupBy(_._1) //2
        .map(p => (p._1,p._2.map(_._2))))) //3
      .map(y => (y._1, y._2.map(z =>(z._1,z._2.sum)))) //4

    val result2 = result1.map(f => (f._1,f._2.toSeq.sortWith(_._2>_._2).take(5)))

    result2.coalesce(1).saveAsTextFile("/home/vinita/Downloads/final_out_tophehe")
  }
}