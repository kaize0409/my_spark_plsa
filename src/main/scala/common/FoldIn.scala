package common

import java.util.Random

import breeze.linalg.{norm, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import topicmodeling.{DocumentParameters, GlobalParameters, TokenEnumerator, PLSA}
import topicmodeling.regulaizers.{SymmetricDirichletTopicRegularizer, SymmetricDirichletDocumentOverTopicDistributionRegularizer}
import org.apache.spark.SparkContext._

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
    val doc_topic_input_path = args(3)
    val query_input_path = args(4)
    val sim_output_path = args(5)

    val plsa = new PLSA(sc,
      numberOfTopics,
      numberOfIterations,
      new Random(13),
      new SymmetricDirichletDocumentOverTopicDistributionRegularizer(0.2f),
      new SymmetricDirichletTopicRegularizer(0.2f))

    val doc_topic_file = sc.textFile(doc_topic_input_path)
    val rawDocuments = doc_topic_file.map(_.split("\t")).filter(_.size > 2).map(x => (x(0), x(1).split(":").toSeq))
    val tokenIndexer = new TokenEnumerator().setRareTokenThreshold(0)
    val tokenIndex = tokenIndexer(rawDocuments) //tokenEnumeration

    //broadcast token index
    val tokenIndexBC = sc.broadcast(tokenIndex)
    val alphabetSize = tokenIndex.alphabet.size

    //retrieve the documentParameters:p(z|d)
    val docParameters = doc_topic_file.map(_.split("\t")).map {
      line => {
        val rawDocument = (line(0), line(1).split(":").toSeq)
        val doc = tokenIndex.transform(rawDocument)
        val theta = Array.fill[Float](numberOfTopics)(0f)
        val topic_score = line(2)
        topic_score.split(";").map {
          x => val splits = x.split(":")
            theta(splits(0).toInt) = splits(1).toFloat
        }
        new DocumentParameters(doc, theta, plsa.documentOverTopicDistributionRegularizer)
      }
    }

    //get the p(w|z)
    val topic_word_file = sc.textFile(topic_word_input_path)
    val topic_word_temp = topic_word_file.map(_.split("\t")).map {
        line => (line(0).toInt, line(1).toInt, line(2).toFloat)
    }.groupBy(_._1).mapValues {
      line => {
        val topic_array = Array.ofDim[Float](alphabetSize)
        line.foreach {
          case (topic_index, word_index, score) => topic_array(word_index) = score
        }
        topic_array
      }
    }.collect()

    val topic_word = Array.fill[Float](numberOfTopics, alphabetSize)(0f)
    topic_word_temp.map {
      case (topic_index, topic_array) => topic_word(topic_index) = topic_array
    }

//    println("topic_word: " + topic_word(0).mkString(","))
    val global = new GlobalParameters(topic_word, alphabetSize)

    //fold in queries
    val query_files = sc.textFile(query_input_path)
    val foldInRawDocs = query_files.map(_.split("\t"))
      .filter(_.size >= 2).map(x => (x(0), x(1).split(":").toSeq))

    // numerate them with the same token index
    val foldInDocs = foldInRawDocs.map(tokenIndexBC.value.transform)

    // now fold in these documents
    val foldedInDocParameters = plsa.foldIn(foldInDocs, global)

    println("fold in queries theta:" + foldedInDocParameters.first().theta.mkString(","))
    println("doc theta:" + docParameters.first().theta.mkString(","))

    //get the similarity
    val similarity = getSimilarity(docParameters, foldedInDocParameters)
    val query_name_list = foldInRawDocs.map(_._1).collect()
    val sim_result = query_name_list.flatMap {
      query => similarity.flatMap(x => x.filter(_._2 == query)).top(20)(Ordering.by(_._3)).map {
        case (docName, queryName, score) => {
          queryName + "\t" + docName + "\t" + score.formatted("%.4f")
        }
      }
    }
    sc.parallelize(sim_result).saveAsTextFile(sim_output_path)

    //format output
    val topic_word_result = topic_word.zipWithIndex.flatMap {
      case (topic, topic_index) => {
        topic.zipWithIndex.sortBy(- _._1).take(30).map {
          case (score, word_index) =>
            topic_index + "\t" + tokenIndex.alphabet.get(word_index) + "\t" + score.formatted("%.4f")
        }
      }
    }
    //sc.parallelize(topic_word_result).saveAsTextFile("/user/hadoop-dataapp/dingkaize/plsa/format_output/topic")
    sc.parallelize(topic_word_result).saveAsTextFile(topic_word_input_path.replaceAll("topic", "format_topic"))

    val doc_topic_result = doc_topic_file.map(_.split("\t")).map {
      line => {
        val theta = Array.fill[Float](numberOfTopics)(0f)
        val dealid = line(0)
        val contents = line(1)
        val topic_score = line(2)
        topic_score.split(";").map {
          x => {
            val splits = x.split(":")
            theta(splits(0).toInt) = splits(1).toFloat
          }
        }
        dealid + "\t" + contents + "\t" + theta.zipWithIndex.sortBy(- _._1).take(20).mkString(";")
      }
    }
    //doc_topic_result.saveAsTextFile("/user/hadoop-dataapp/dingkaize/plsa/format_output/doc")
    doc_topic_result.saveAsTextFile(doc_topic_input_path.replaceAll("doc", "format_doc"))

    sc.stop()

  }

  def getSimilarity(docParameters : RDD[DocumentParameters], foldedInDocParameters : RDD[DocumentParameters]) = {
    val bcFolded = docParameters.context.broadcast(foldedInDocParameters.collect())
    val result = docParameters.map {
      documentParameter => bcFolded.value.map(dp =>
        (documentParameter.document.docName, dp.document.docName, sim(dp.theta, documentParameter.theta)))
    }
    //每个doc跟各个query的相似度
    result
  }

  def sim(a : Array[Float], b : Array[Float]): Float = {
    val va = DenseVector(a)
    val vb = DenseVector(b)
    va dot vb / (norm(va) * norm(vb)).toFloat
  }

}
