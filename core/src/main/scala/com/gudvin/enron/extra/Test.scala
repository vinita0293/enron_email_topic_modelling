package com.gudvin.enron.extra

import com.gudvin.enron.utils.Attribute

import scala.reflect.runtime.universe._
/**
  * Created by vinita on 7/7/16.
  */
object Test {
  def main(args: Array[String]) {
    typeOf[Attribute].members.map(_.toString.split(" ")).filter(_(0).equals("value")).map(_(1)).toSeq.distinct.reverse
      .foreach(println)
  }

}
