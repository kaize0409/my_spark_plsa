package common

import java.util.Random

import breeze.linalg.{norm, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import topicmodeling.regulaizers.{SymmetricDirichletDocumentOverTopicDistributionRegularizer, SymmetricDirichletTopicRegularizer}
import topicmodeling.{DocumentParameters, PLSA, TokenEnumerator}

/**
 * Created by kaiserding on 15/3/18.
 */
object PLSAModel {

  def runPLSA(plsa : PLSA, sc : SparkContext, doc_input_path : String, query_input_path : String,
              topic_output_path : String, doc_output_path : String) {

    val files = sc.textFile(doc_input_path)
    /*val splitted = files.map(_.split("\t"))
    println(s"# size = 2: ${splitted.filter(_.size > 1).size}")
    println(s"# size = 1: ${splitted.filter(_.size == 1).size}")*/

    // the data in text form
    val rawDocuments = files.map(_.split("\t"))
      .filter(_.size >= 2).map(x => (x(0), x(1).split(":").toSeq))

    // use token indexer to generate tokenIndex
    val tokenIndexer = new TokenEnumerator().setRareTokenThreshold(0)
    val tokenIndex = tokenIndexer(rawDocuments) //tokenEnumeration

    //broadcast token index
    val tokenIndexBC = sc.broadcast(tokenIndex)

    val docs = rawDocuments.map(tokenIndexBC.value.transform)
    //val docs = rawDocuments.map(x => tokenIndexBC.value.transform(x._1, x._2))

    // train plsa
    val (docParameters, global) = plsa.infer(docs)

    // this matrix is an array of topic distributions over words
    val phi = global.phi

    // let's suppose there are some more documents

    val queries = sc.textFile(query_input_path).collect()
    val foldInRawDocs = sc.parallelize(queries.map(_.split("\t"))
      .filter(_.size >= 2).map(x => (x(0), x(1).split(":").toSeq)))

    // numerate them with the same token index
    val foldInDocs = foldInRawDocs.map(tokenIndexBC.value.transform)

    println(s"foldindocs size: ${foldInDocs.collect().size}")
    // now fold in these documents
    val foldedInDocParameters = plsa.foldIn(foldInDocs, global)

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
      case (topic, topic_index) => topic.zipWithIndex.map {
        case (value, word_index) => {
          val word = tokenIndex.alphabet.get(word_index)
          topic_index + "\t" + word + "\t" + value.formatted("%.4f")
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

    val doc_topic = docParameters.flatMap {
      val str = new StringBuilder()
      documentParameter => documentParameter.theta.zipWithIndex.map {
        case (value, topic_index) => {
          if (str.length == 0) {
            str.append(topic_index + ":" + value)
          }
        }
      }
    }

    doc_topic.saveAsTextFile(doc_output_path)

    val query_topic = foldedInDocParameters.flatMap {
      documentParameter => documentParameter.theta.zipWithIndex.filter(_._1 > 1e-4f).map {
        case (value, topic_index) => {
          documentParameter.document.docName + "\t" + topic_index + "\t" + value.formatted("%.4f")
        }
      }
    }

    query_topic.saveAsTextFile(doc_output_path.replace("doc", "query"))

    val similarity = getSimilarity(docParameters, foldedInDocParameters)

    val query_name_list = foldInRawDocs.map(_._1).collect()

    val result = query_name_list.flatMap { query =>
       similarity.flatMap(x => x.filter(_._2 == query)).top(20)(Ordering.by(_._3)).map {
         case (docName, queryName, score) => {
           queryName + "\t" + docName + "\t" + score
         }
       }
    }

    sc.parallelize(result).saveAsTextFile(doc_output_path.replace("doc", "sim"))

    //formatResult(rawDocuments, phi, docParameters.map(par => (par.document.docName, par.theta)))

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

  def main(args: Array[String]) {

    if (args.length != 6) {
      println("usage: numberOfTopics, numberOfIterations, doc_input_path, " +
        "query_input_path, topic_output_path, doc_output_path")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PLSA Application")
    val sc = new SparkContext(conf)

    val numberOfTopics = args(0).toInt
    val numberOfIterations = args(1).toInt

    val doc_input_path = args(2)
    val query_input_path = args(3)
    val topic_output_path = args(4)
    val doc_output_path = args(5)

    val plsa = new PLSA(sc,
      numberOfTopics,
      numberOfIterations,
      new Random(13),
      new SymmetricDirichletDocumentOverTopicDistributionRegularizer(0.2f),
      new SymmetricDirichletTopicRegularizer(0.2f))

    runPLSA(plsa, sc, doc_input_path, query_input_path,topic_output_path, doc_output_path)

    sc.stop()
  }
}
