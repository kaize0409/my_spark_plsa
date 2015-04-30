package common

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by kaiserding on 15/4/17.
 */
object FormatOutput {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("PLSA Application format output")
    val sc = new SparkContext(conf)

    if (args.length != 5) {
      println("usage: doc_input_path, topic_word_path, doc_topic_path" +
        "format_topic_word_output_path, format_doc_topic_output_path")
      System.exit(1)
    }

    val input_path = args(0)
    val topic_path = args(1)
    val doc_path = args(2)
    val format_topic_output_path = args(3)
    val format_doc_output_path = args(4)

    val doc_file = sc.textFile(doc_path)
    val topic_file = sc.textFile(topic_path)
    val input_file = sc.textFile(input_path)

    val content_map = input_file.map {
      line => {
        val splits = line.split("\t")
        val dealid = splits(0)
        val words = splits(1)
        (dealid, words)
      }
    }.collect().toMap[String, String]

    val doc_map = doc_file.map {
      line => {
        val contents = line.split("\t")//0:dealid;1:topic;2:score
        (contents(0), List((contents(1), contents(2).toFloat)))
      }
    }.reduceByKey((val1, val2) => val1 ++ val2).collect().toMap

    val doc_result = new ArrayBuffer[String]()
    for ((key, value) <- doc_map) {
      val str = new StringBuilder()
      str.append(key + "\t" + content_map(key) + "\t")

      val valueSorted = value.sortBy(- _._2)
      for (tmp <- valueSorted) {
        str.append(tmp._1 + ":" + tmp._2 + ",")
      }
      doc_result.append(str.deleteCharAt(str.size - 1).toString())
    }

    val topic_map = topic_file.map {
      line => {
        val splits = line.split("\t")
        val topicid = splits(0)
        val word = splits(1)
        val score = splits(2).toFloat
        (topicid, List((word, score)))
      }
    }.reduceByKey((val1, val2) => val1 ++ val2).collect().toMap

    val topic_result = new ArrayBuffer[String]()
    for ((key, value) <- topic_map) {
      val str = new StringBuilder()
      str.append(key + "\t")

      val valueSorted = value.sortBy(- _._2).take(30)
      for (tmp <- valueSorted) {
        str.append(tmp._1 + ":" + tmp._2 + ",")
      }
      topic_result.append(str.deleteCharAt(str.size - 1).toString())
    }

    sc.parallelize(topic_result).saveAsTextFile(format_topic_output_path)
    sc.parallelize(doc_result).saveAsTextFile(format_doc_output_path)
    sc.stop()
  }
}
