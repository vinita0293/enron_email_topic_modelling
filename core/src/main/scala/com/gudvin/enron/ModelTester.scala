
package com.gudvin.enron

import com.gudvin.enron.utils.EnronUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 7/17/16.
  */
object ModelTester {

  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "spark://vinita-Lenovo-G50-80:7077"

    val conf: SparkConf = new SparkConf()
      .setAppName("Enron starting")
      .setMaster("local")
      .setSparkHome(sparkHome)

    val sc = new SparkContext(conf)


    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val testingMessagesRDD = sc.wholeTextFiles("/media/vinita/Projects/study_related/Data/enron/testing")
      .map(x => (x._1, x._2.replace("\n", ""))).map(EnronUtils.EnronParser)
      .map(record => (record.Message_ID, record.Message))
      .zipWithIndex().map(_.swap).cache()

    testingMessagesRDD.cache()

    val df = testingMessagesRDD.map(_._2._2).toDF("docs")

    val testingVectorizedModel = getVectorizedModel(df, 50)
    val vocabularyArray = testingVectorizedModel.stages(2).asInstanceOf[CountVectorizerModel].vocabulary

    val testingCorpus = testingVectorizedModel.transform(df)
      .select("features")
      .rdd
      .map { case Row(features: org.apache.spark.mllib.linalg.Vector) => features }
      .zipWithIndex()
      .map(_.swap)

    val loadedModel = DistributedLDAModel.load(sc, "myLDAModel")

    loadedModel.toLocal.topicDistributions(testingCorpus).foreach {
      case (term, weight) => println(vocabularyArray(term.toInt))
    }

  }

  def getVectorizedModel(df: DataFrame, vocabularySize: Int) = {

    val tokenizer = new RegexTokenizer()
      .setInputCol("docs")
      .setOutputCol("rawTokens")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("rawTokens")
      .setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords)
    val countVectorizer = new CountVectorizer()
      .setVocabSize(vocabularySize)
      .setInputCol("tokens")
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    pipeline.fit(df)
  }
}