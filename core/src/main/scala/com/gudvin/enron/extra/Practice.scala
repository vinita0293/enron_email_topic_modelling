package com.gudvin.enron.extra

import com.gudvin.enron.utils.EnronUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by vinita on 7/6/16.
  */

object Practice {

  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "spark://vinita-Lenovo-G50-80:7077"

    val conf: SparkConf = new SparkConf()
      .setAppName("Enron starting")
      .setMaster("local")
      .setSparkHome(sparkHome)

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val loadedFileRDD: RDD[(String, String)] = sc.wholeTextFiles("/home/vinita/Downloads/enron_example/", 6)
      /*.map(x => x._2.split("\r").map(line=> (x._1,line) ).toSeq)
      .flatMap(identity)
      .repartition(26)

    p1,l1
    p1,l2
    p1,l3
    p2,l1
    p2,l2

    p1,((p1,l1),(p1,l2),(p1,l3))
    p2,(p2,(l2,l2))


    val newRDD=loadedFileRDD.groupBy(_._1).map(x => (x._1,x._2.map(_._2).mkString("\r")))*
    //val newRDD=loadedFileRDD.groupBy(_._1).map(x => (x._1,x._2.map(_._2).mkString("\r").replace("\n","")))
    //val df = loadedFileRDD.zipWithIndex().map(x => Person(x._1,x._2)).toDF()
    newRDD.cache()

    newRDD.foreach(println)*/
   //val sqlContext = new SQLContext(sc)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import hiveContext.implicits._// by using this we are able to dataframe in the code
    val newRDD=loadedFileRDD.map(x => (x._1,x._2.replace("\n","")))
    val resultantRDD = newRDD.map(EnronUtils.EnronParser)
    val resultantDF = resultantRDD.toDF()
    //resultantDF.registerTempTable("enron")
   // resultantRDD.saveAsTextFile("EnronData")
    //hiveContext.sql("select * from enron").show()
    resultantDF.write.saveAsTable("enron123")

  }


}