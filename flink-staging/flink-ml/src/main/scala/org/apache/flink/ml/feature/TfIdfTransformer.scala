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

import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.SparseVector

import scala.collection.mutable.LinkedHashSet
import scala.math.log;

/**
 * This transformer calcuates the term-frequency times inverse document frequency for the give DataSet of documents.
 * The DataSet will be treated as the corpus of documents. The single words will be filtered against the regex:
 * <code>
 * (?u)\b\w\w+\b
 * </code>
 * <p>
 * The TF is the frequence of a word inside one document
 * <p>
 * The IDF for a word is calculated: log(total number of documents / documents that contain the word) + 1
 * <p>
 * This transformer returns a SparseVector where the index is the hash of the word and the value the tf-idf.
 * @author Ronny Bräunlich
 * @author Vassil Dimov
 * @author Filip Perisic
 */
class TfIdfTransformer extends Transformer[(Int, Seq[String]), (Int, SparseVector)] {

  override def transform(input: DataSet[(Int /* docId */ , Seq[String] /*The document */ )], transformParameters: ParameterMap): DataSet[(Int, SparseVector)] = {

    val matchedWordCounts = input.flatMap(t => {
      t._2.flatMap(s => {
        "(?u)\\b\\w\\w+\\b".r findAllIn s map (
          s2 => (t._1, s2.toLowerCase, 1)
          )
      })
    })
      .filter(t => !transformParameters.apply(StopWordParameter).contains(t._2))
      //group by document and word
      .groupBy(0, 1)
      // calculate the occurrence count of each word in specific document
      .sum(2)

    val dictionary = matchedWordCounts
      .map(t => LinkedHashSet(t._2))
      .reduce((set1, set2) => set1 ++ set2)
      //to make sure we only create one dictionary
      .setParallelism(1)
      .map(set => set.zipWithIndex.toMap)
      .flatMap(m => m.toList)

    val numberOfWords = matchedWordCounts
      .map(t => (t._2))
      .distinct(t => t)
      .map(t => 1)
      .reduce(_ + _);

    val idf: DataSet[(String, Double)] = calculateIDF(matchedWordCounts)
    val tf: DataSet[(Int, String, Int)] = matchedWordCounts

    // docId, word, tfIdf
    val tfIdf = tf.join(idf).where(1).equalTo(0) {
      (t1, t2) => (t1._1, t1._2, t1._3.toDouble * t2._2)
    }

    val res = tfIdf.crossWithTiny(numberOfWords)
      // docId, word, tfIdf, numberOfWords
      .map(t => (t._1._1, t._1._2, t._1._3, t._2))
      //assign every word its position
      .joinWithTiny(dictionary).where(1).equalTo(0)
      //join the tuples (docId, word, tfIdf, numberOfWords, index
      .map(t => (t._1._1, t._1._2, t._1._3, t._1._4, t._2._2))
      .map(t => {
      (t._1, List[(Int, Double)]((t._5, t._3)), t._4)
    })
      .groupBy(t => t._1)
      .reduce((t1, t2) => (t1._1, t1._2 ++ t2._2, t1._3))
      .map(t => (t._1, SparseVector.fromCOO(t._3, t._2.toIterable)))

    res
  }

  /**
   * Takes the DataSet of DocId, Word and WordCount and calculates the IDF as tuple of word and idf
   * @param set
   */
  private def calculateIDF(set: DataSet[(Int, String, Int)]) = {
    val totalNumberOfDocuments = set
      //map the input only to the docId
      .map(t => t._1)
      .groupBy(i => i)
      //reduce to unique docIds
      .reduce((t1, t2) => t1)
      .count();

    val idf = set
      //for tuple docId, Word and wordcount only take the word and a 1
      .map(t => (t._2, 1))
      //group by word
      .groupBy(t => t._1)
      //and count the documents
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      //calculate IDF
      .map(t => (t._1, log(totalNumberOfDocuments.toDouble / t._2.toDouble) + 1.0))
    idf
  }
}

object StopWordParameter extends Parameter[Set[String]] with Serializable {
  override val defaultValue: Option[Set[String]] = Some(Set())
}