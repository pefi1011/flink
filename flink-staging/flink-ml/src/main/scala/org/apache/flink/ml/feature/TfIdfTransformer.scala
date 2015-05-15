package org.apache.flink.ml.feature

import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.SparseVector
import scala.math.log

/**
 * @author Ronny Bräunlich
 */
class TfIdfTransformer extends Transformer[(Int, Seq[String]), (Int, SparseVector)] {

  override def transform(input: DataSet[(Int, Seq[String])], transformParameters: ParameterMap): DataSet[(Int, SparseVector)] = {
    val wordCounts = input
      //count the words
      .flatMap(t => {
        //create tuple docId, word, count
        t._2.map(s => (t._1, s, 1))
      })
      //group by document and word
      .groupBy(0, 1)
      //add the numbers
      .reduce((tup1, tup2) => (tup1._1, tup1._2, tup1._3 + tup2._3))
    val idf: DataSet[(String, Double)] = calculateIDF(wordCounts)
    val tf: DataSet[(Int, String, Int)] = calculateTF(wordCounts)

    // docId, word, tfIdf
    val tfIdf = tf.join(idf).where(1).equalTo(0) {
      (t1, t2) => (t1._1, t1._2, t1._3 * t2._2)
    }

    //not sure how to work with SparseVector, this doesn't work...
    tfIdf
      .map(t => (t._1, SparseVector.fromCOO(1, (t._2.hashCode, t._3))))
      .groupBy(t => t._1)
      .reduce((t1, t2) =>
        (t1._1,
         SparseVector.fromCOO(t1._2.size + t2._2.size,t1._2.toSeq ++ t2._2.toSeq)))
    //the sequence are the documents and we have to transform them into bag of words
    //TF IDF across the corpus
  }

  private def calculateTF(wordCounts: DataSet[(Int, String, Int)]): DataSet[(Int, String, Int)] = {
    val mostOftenWord = wordCounts
      //reduce to the count only
      .map(t => t._3)
      //take the biggest one
      .reduce((nr1, nr2) => if (nr1 > nr2) nr1 else nr2)
      .first(1)
    val tf = wordCounts
      //combine with the most often word
      .crossWithTiny(mostOftenWord)
      //create one tuple for easier handling (docId, Word, count, most often word)
      .map(t => (t._1._1, t._1._2, t._1._3, t._2))
      //calculate the tf (docId, word, tf)
      .map(t => (t._1, t._2, t._3 / t._4))
    tf
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
      //reduce to unique doc IDs
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
      .map( t => (t._1, log(totalNumberOfDocuments / t._2)))
    idf
  }
}

object StopWordParameter extends Parameter[List[String]] {
  override val defaultValue: Option[List[String]] = Some(List())
}