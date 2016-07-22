
package com.gudvin.enron

import com.gudvin.enron.utils.EnronUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 7/17/16.
  */
object TopicModeller {

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
    val numTopics = 20
    //val loadedFileRDD: RDD[(String, String)] =
    //sc.wholeTextFiles("/home/vinita/Downloads/maildir/*/{_sent_mail/,sent}*", 12)

   /* val loadedFileRDD: RDD[(String, String)] =
      sc.wholeTextFiles("/home/vinita/Downloads/enron_example", 12)

    val newRDD = loadedFileRDD.map(x => (x._1, x._2.replace("\n", "")))
    val resultantRDD = newRDD.map(EnronUtils.EnronParser)
    resultantRDD.cache()*/


    /** -VM Option
      * -XX:MaxPermSize=1024m
      *
      * Featuring of documents Or Vectorization
      * doc1 = a b c d
      * doc2 = a b e g
      * doc3 = f e a c
      *
      * training
      *coordinates: a b c d e f g
      *       a b c d e f g
      * doc1  1 1 1 1 0 0 0     Vector[1111000]
      * doc2  1 1 0 0 1 0 1     .
      * doc3  1 0 1 0 1 1 0     .
      *
      * training
      * doc1 weight(a) - 0.562, weight(b) - 0.4 .....
      *
      *
      *
      */

    /**
      * TF -> IDF -> HashingTF
      */

    val trainingMessagesRDD = sc.wholeTextFiles("/media/vinita/Projects/study_related/Data/enron/training")
      .map(x => (x._1, x._2.replace("\n", ""))).map(EnronUtils.EnronParser)
      .map(record => (record.Message_ID, record.Message))
      .zipWithIndex().map(_.swap).cache()

    trainingMessagesRDD.cache()

    val df = trainingMessagesRDD.map(_._2._2).toDF("docs")

    val trainingVectorizedModel = getVectorizedModel(df, 50)
    val vocabularyArray = trainingVectorizedModel.stages(2).asInstanceOf[CountVectorizerModel].vocabulary

    val trainingCorpus = trainingVectorizedModel.transform(df)
      .select("features")
      .rdd
      .map { case Row(features: org.apache.spark.mllib.linalg.Vector) => features }
      .zipWithIndex()
      .map(_.swap)

    val lda = new LDA().setK(numTopics).setMaxIterations(50)

    val ldaModel = lda.run(trainingCorpus)
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)


    topicIndices.foreach { case (terms, termWeights) =>
      println("TOPIC:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${vocabularyArray(term.toInt)}\t$weight")
      }
      println()
    }

    // Save and load model.
    ldaModel.save(sc, "myLDAModel")
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