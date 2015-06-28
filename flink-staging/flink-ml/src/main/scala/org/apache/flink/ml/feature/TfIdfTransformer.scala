/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.feature

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.SparseVector

import scala.collection.mutable.LinkedHashSet
import scala.math.log

/**
 * This transformer calculates the term-frequency times inverse document frequency for the give DataSet of documents.
 * The DataSet will be treated as the corpus of documents. The single words will be filtered against the regex:
 * <code>
 * (?u)\b\w\w+\b
 * </code>
 * <p>
 * The TF is the frequency of a word inside one document
 * <p>
 * The IDF for a word is calculated: log(total number of documents / documents that contain the word) + 1
 * The formula contains "+1", terms with a zero IDF don't get get completely ignored.
 * <p>
 * This transformer returns a SparseVector where the index is the hash of the word and the value the tf-idf.
 */
class TfIdfTransformer extends Transformer[(Int, Seq[String]), (Int, SparseVector)] {

  override def transform(input: DataSet[(Int /* docId */ , Seq[String] /*The document */ )], transformParameters: ParameterMap): DataSet[(Int, SparseVector)] = {

    val matchedWordCounts = input.flatMap(inputEntry => {
      inputEntry._2.flatMap(textLine => {
        "(?u)\\b\\w\\w+\\b".r findAllIn textLine map (matchedWord => (inputEntry._1, matchedWord.toLowerCase, 1))
      })
    })
      .filter(wordInfo => !transformParameters.apply(StopWordParameter).contains(wordInfo._2))
      //group by document and word
      .groupBy(0, 1)
      // calculate the occurrence count of each word in specific document
      .sum(2)

    val dictionary = matchedWordCounts
      .map(t => LinkedHashSet(t._2))
      .reduce((set1, set2) => set1 ++ set2)
      .map(set => set.zipWithIndex)
      .flatMap(m => m.toList)

    val numberOfWords = matchedWordCounts
      .map(t => t._2)
      .distinct(t => t)
      .map(t => 1)
      .reduce(_ + _)

    val idf: DataSet[(String, Double)] = calculateIDF(matchedWordCounts)
    val tf: DataSet[(Int, String, Int)] = matchedWordCounts

    // docId, word, tfIdf
    val tfIdf = tf.join(idf).where(1).equalTo(0) {
      (t1, t2) => (t1._1, t1._2, t1._3.toDouble * t2._2)
    }

    val res = tfIdf.map(new RichMapFunction[Tuple3[Int, String, Double], Tuple2[Tuple3[Int, String, Double], Int]]() {

      var broadcastNumberOfWords: java.util.List[(Int)] = null

      override def open(config: Configuration): Unit = {
        broadcastNumberOfWords = getRuntimeContext().getBroadcastVariable[Int]("broadcastSetName")
      }

      def map(in: Tuple3[Int, String, Double]): Tuple2[Tuple3[Int, String, Double], Int] = {
        ((in._1, in._2, in._3), broadcastNumberOfWords.get(0))
      }

    }).withBroadcastSet(numberOfWords, "broadcastSetName")
      // docId, word, tfIdf, numberOfWords
      .map(wordInfo => (wordInfo._1._1, wordInfo._1._2, wordInfo._1._3, wordInfo._2))
      //assign every word its position
      .joinWithTiny(dictionary).where(1).equalTo(0)
      //join the tuples (docId, word, tfIdf, numberOfWords, index
      .map(wordInfoWithIndex => (wordInfoWithIndex._1._1, wordInfoWithIndex._1._2, wordInfoWithIndex._1._3, wordInfoWithIndex._1._4, wordInfoWithIndex._2._2))
      .map(t => {
      (t._1, List[(Int, Double)]((t._5, t._3)), t._4)
    })
      .groupBy(t => t._1)
      .reduce((t1, t2) => (t1._1, t1._2 ++ t2._2, t1._3))
      .map(t => (t._1, SparseVector.fromCOO(t._3, t._2.toIterable)))

    // Broadcast variable

    // 1. The DataSet to be broadcasted
    //    val toBroadcast = env.fromElements(1, 2, 3)
    //
    //    val data = env.fromElements("a", "b")
    //
    //    data.map(new RichMapFunction[String, String]() {
    //      var broadcastSet: Traversable[String] = null
    //
    //      override def open(config: Configuration): Unit = {
    //        // 3. Access the broadcasted DataSet as a Collection
    //        broadcastSet = getRuntimeContext().getBroadcastVariable[String]("broadcastSetName").asScala
    //      }
    //
    //      def map(in: String): String = {
    //        ...
    //      }
    //    }).withBroadcastSet(toBroadcast, "broadcastSetName") // 2. Broadcast the DataSet
    //

    // return result
    res
  }

  /**
   * Takes the DataSet of DocId, Word and WordCount and calculates the IDF as tuple of word and idf
   * @param wordInfosPerDoc DocId, Word and WordCount
   */
  private def calculateIDF(wordInfosPerDoc: DataSet[(Int, String, Int)]) = {
    val totalNumberOfDocuments = wordInfosPerDoc
      //map the input only to the docId
      .map(wordInfoPerDoc => wordInfoPerDoc._1)
      .distinct(t => t)
      .count()

    // set is the calculated idf
    wordInfosPerDoc
      //for tuple docId, Word and wordCount only take the word and a 1
      .map(wordInfoPerDoc => (wordInfoPerDoc._2, 1))
      //group by word
      .groupBy(word => word._1)
      //and count the documents
      .reduce((word1, word2) => (word1._1, word1._2 + word2._2))
      //calculate IDF
      .map(docCountByWord => (docCountByWord._1, log(totalNumberOfDocuments.toDouble / docCountByWord._2.toDouble) + 1.0))

  }
}

object StopWordParameter extends Parameter[Set[String]] with Serializable {
  override val defaultValue: Option[Set[String]] = Some(Set())
}