/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package topicmodeling


import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import scala.io.Source

trait AbstractTopicModelSuite[DocumentParameterType <: DocumentParameters,
GlobalParameterType <: GlobalParameters] extends FunSuite {

    val sc = new SparkContext("local[4]", "test plsa")

    val EPS = 1e-5f

    def testPLSA(plsa: TopicModel[DocumentParameterType, GlobalParameterType]) {



        val files = Source.fromFile("data/file1").getLines.toSeq
        //val rawDocuments = sc.parallelize(files.map(_.split(" ").toSeq))
        val rawDocuments = sc.parallelize(files.map(_.split("\001")).filter(_.size == 2).map(
          x => (x(0), x(1).split(":").toSeq))
        )

        // the data in text form
        //val rawDocuments = sc.parallelize(Seq("a b a", "x y y z", "a b z x ").map(_.split(" ").toSeq))

        val tokenIndexer = new TokenEnumerator().setRareTokenThreshold(0)

        // use token indexer to generate tokenIndex
        val tokenIndex = tokenIndexer(rawDocuments)//tokenEnumeration

        //broadcast token index
        val tokenIndexBC = sc.broadcast(tokenIndex)

        val docs = rawDocuments.map(tokenIndexBC.value.transform)
        //val docs = rawDocuments.map(x => tokenIndexBC.value.transform(x._1, x._2))
        val docs_value = docs.collect()

        // train plsa
        val (docParameters, global) = plsa.infer(docs)

        // this matrix is an array of topic distributions over words
        val phi = global.phi

        // thus, every its row should sum up to one
        var writer = new PrintWriter("result/topic")
        /*for (topic <- phi) {
            for (x <- topic) {
                writer.println(x)
            }
            assert(doesSumEqualToOne(topic), "phi matrix is not normalized")
        }*/
        phi.zipWithIndex.foreach {
            case (topic, topic_index) => topic.zipWithIndex.foreach {
                case (value, word_index) => {
                    val word = tokenIndex.alphabet.get(word_index)
                    if (value >= 1e-4f) {
                      writer.println(f"$topic_index%d, $word%s, $value%.4f")
//                      writer.format("%d\t%s\t%.5f\n", topic_index, word, value)
                    }
                }
            }
        }
        writer.close()

       assert(phi.forall(_.forall(_ >= 0f)), "phi matrix is non-non-negative")

        // a distribution of a document over topics (theta) should also sum up to one
        writer = new PrintWriter("result/doc")
        for (documentParameter <- docParameters.collect) {
            documentParameter.theta.zipWithIndex.foreach {
                case (value, topic_index) => {
                    if (value >= 1e-4f) {
                      writer.println(f"${documentParameter.document.docName}%s, $topic_index%d, $value%.4f")
//                      writer.format("%s\t%d\t%.5f\n", documentParameter.document.docName, topic_index, value)
                    }
                }
            }

            assert(doesSumEqualToOne(documentParameter.theta), "theta is not normalized")
        }
        writer.close()
       assert(docParameters.collect.forall(_.theta.forall(_ >= 0f)), "theta is not non-negative")

        /*



        // the same requirements of non-negativeness and normalization apply
        assert(foldedInDocParameters.collect.forall(_.theta.forall(_ >= 0f)),
            "theta for folded in docs is not non-negative")

        for (documentParameter <- docParameters.collect)
            assert(doesSumEqualToOne(documentParameter.theta),
                "theta for folded in docs  is not normalized")
        */
    }

    private def doesSumEqualToOne(arr: Array[Float]) = math.abs(arr.sum - 1) < EPS

}
