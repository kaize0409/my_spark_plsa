package common

import java.util.Random

import org.apache.spark.{SparkContext, SparkConf}
import topicmodeling.{GlobalParameters, TokenEnumerator, PLSA}
import topicmodeling.regulaizers.{SymmetricDirichletTopicRegularizer, SymmetricDirichletDocumentOverTopicDistributionRegularizer}

/**
 * Created by kaiserding on 15/4/30.
 */
object FoldIn {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("PLSA Application Fold in queries")
    val sc = new SparkContext(conf)


    val numberOfTopics = args(0).toInt
    val numberOfIterations = args(1).toInt
    val topic_word_input_path = args(2)
    val doc_topic_input_path =args(3)
    val doc_input_path = args(4)

    val plsa = new PLSA(sc,
      numberOfTopics,
      numberOfIterations,
      new Random(13),
      new SymmetricDirichletDocumentOverTopicDistributionRegularizer(0.2f),
      new SymmetricDirichletTopicRegularizer(0.2f))

    val files = sc.textFile(doc_input_path)
    val rawDocuments = files.map(_.split("\t"))
      .filter(_.size >= 2).map(x => (x(0), x(1).split(":").toSeq))
    val tokenIndexer = new TokenEnumerator().setRareTokenThreshold(0)
    val tokenIndex = tokenIndexer(rawDocuments) //tokenEnumeration
    val alphabetSize = tokenIndex.alphabet.size

    val topic_word = Array.fill[Float](numberOfTopics, alphabetSize)(0f)

    val topic_word_file = sc.textFile(topic_word_input_path)
    topic_word_file.map(_.split("\t")).map {
      x => topic_word(x(0).toInt)(x(1).toInt) = x(2).toFloat
    }

    val global = new GlobalParameters(topic_word, alphabetSize)


  }

}
