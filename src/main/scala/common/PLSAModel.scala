package common

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import topicmodeling.regulaizers.{SymmetricDirichletDocumentOverTopicDistributionRegularizer, SymmetricDirichletTopicRegularizer}
import topicmodeling.{PLSA, TokenEnumerator}

/**
 * Created by kaiserding on 15/3/18.
 */
object PLSAModel {

  def runPLSA(plsa: PLSA, sc: SparkContext, doc_input_path: String,
              topic_output_path: String, doc_output_path: String) {

    val files = sc.textFile(doc_input_path)
    /*val splitted = files.map(_.split("\t"))
    println(s"# size = 2: ${splitted.filter(_.size > 1).size}")
    println(s"# size = 1: ${splitted.filter(_.size == 1).size}")*/

    // the data in text form
    val rawDocuments = files.map(_.split("\t"))
      .filter(_.size >= 2).map(x => (x(0), x(1).split(":").toSeq))


    //get the alphabet
    /*val wordTable = rawDocuments.flatMap(x => x._2).map((_, 1)).reduceByKey(_ + _).map {
      case (word, num) => word + "\t" + num
    }
    wordTable.saveAsTextFile("/user/hadoop-dataapp/dingkaize/plsa/alphabet")
*/


    // use token indexer to generate tokenIndex
    val tokenIndexer = new TokenEnumerator().setRareTokenThreshold(15)
    val tokenIndex = tokenIndexer(rawDocuments) //tokenEnumeration


    //broadcast token index
    val tokenIndexBC = sc.broadcast(tokenIndex)

    val docs = rawDocuments.map(tokenIndexBC.value.transform)
    //val docs = rawDocuments.map(x => t okenIndexBC.value.transform(x._1, x._2))

    // train plsa
    val (docParameters, global) = plsa.infer(docs)

    // this matrix is an array of topic distributions over words
    val phi = global.phi

    //output the result
    /*val topic_word = phi.zipWithIndex.flatMap {
      case (topic, topic_index) => topic.zipWithIndex.filter(_._1 > 1e-4f).map {
        case (value, word_index) => {
          val word = tokenIndex.alphabet.get(word_index)
            topic_index + "\t" + word + "\t" + value.formatted("%.4f")
        }
      }
    }*/

    //output topic_word
    val topic_word = phi.zipWithIndex.flatMap {
      case (topic, topic_index) => topic.zipWithIndex.filter(_._1 > 0f).map {
        case (value, word_index) => {
          topic_index + "\t" + word_index + "\t" + value
        }
      }
    }
    sc.parallelize(topic_word).saveAsTextFile(topic_output_path)

    //output doc_topic
    /*val doc_topic = docParameters.flatMap {
      documentParameter => documentParameter.theta.zipWithIndex.filter(_._1 > 1e-4f).map {
        case (value, topic_index) => {
          documentParameter.document.docName + "\t" + topic_index + "\t"  + value.formatted("%.4f")
        }
      }
    }*/

    val doc_topic = docParameters.map {
      documentParameter => {
        val str = new StringBuilder()
        documentParameter.theta.zipWithIndex.filter(_._1 > 0f).map {
          case (value, topic_index) => {
            if (str.length != 0) {
              str.append(";")
            }
            str.append(topic_index + ":" + value)
          }
        }
        documentParameter.document.docName + "\t" + documentParameter.document.contents.mkString(":") + "\t" + str.toString()
      }
    }

    doc_topic.saveAsTextFile(doc_output_path)


  }

  def main(args: Array[String]) {

    if (args.length != 5) {
      println("usage: numberOfTopics, numberOfIterations, doc_input_path, topic_output_path, doc_output_path")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PLSA Application")
    val sc = new SparkContext(conf)

    val numberOfTopics = args(0).toInt
    val numberOfIterations = args(1).toInt

    val doc_input_path = args(2)
    val topic_output_path = args(3)
    val doc_output_path = args(4)

    val plsa = new PLSA(sc,
      numberOfTopics,
      numberOfIterations,
      new Random(13),
      new SymmetricDirichletDocumentOverTopicDistributionRegularizer(0.2f),
      new SymmetricDirichletTopicRegularizer(0.2f))

    runPLSA(plsa, sc, doc_input_path, topic_output_path, doc_output_path)

    sc.stop()
  }
}
