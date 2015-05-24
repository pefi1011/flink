package org.apache.flink.ml.feature

import akka.routing.MurmurHash
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.SparseVector
import scala.math.log
import scala.util.hashing.MurmurHash3

/**
 * @author Ronny Bräunlich
 */
class TfIdfTransformer extends Transformer[(Int, Seq[String]), (Int, SparseVector)] {

  override def transform(input: DataSet[(Int /* docId */, Seq[String] /*The document */)], transformParameters: ParameterMap): DataSet[(Int, SparseVector)] = {

    // Here we will store the words in he form (docId, word, count)
    // Count represent the occurrence of "word" in document "docId"
    val wordCounts = input
      //count the words
      .flatMap(t => {
      //create tuples docId, word, 1
      t._2.map(s => (t._1, s, 1))
    })
      //group by document and word
      .groupBy(0, 1)
      // calculate the occurrence count of each word in specific document
      .sum(2)

    println(wordCounts.collect())

    // TODO Change this implementation
    val words = input
      //count the words
      .flatMap(t => {
      //create tuples docId, word, 1
      t._2.map(s => (s, 1))
    })
      //group by document and word
      .groupBy(0)
      // calculate the occurrence count of each word in specific document
      .sum(1)

    val wordsCount = words.collect().length

    println("WC " + wordsCount)

    val idf: DataSet[(String, Double)] = calculateIDF(wordCounts)
    val tf: DataSet[(Int, String, Double)] = calculateTF(wordCounts)

    // docId, word, tfIdf
    val tfIdf = tf.join(idf).where(1).equalTo(0) {
      (t1, t2) => (t1._1, t1._2, t1._3 * t2._2)
    }

    // TODO Delete these lines (only implementation purposes)
    val resTF = tf.collect()
    val resIDF = idf.collect()
    val resTfIdf = tfIdf.collect()
    println("ResTF   ------->")
    for ( tf <- resTF) {
      print(" " + tf.toString())
    }
    println()
    println("ResIDF  ------->")
    for ( idf <- resIDF) {
      print(" " + idf.toString())
    }
    println()
    println("ResTfID ------->")
    for ( idf <- resTfIdf) {
      print(" " + idf.toString())
    }
    println()
    // END

    //var test = (1, new SparseVector(4, Array(0, 1, 2, 3), Array(0.0, 0.0, 0.0, 0.0)))


    //not sure how to work with SparseVector, this doesn't work...
    // tfIdf ----> // docId, word, tfIdf

    var prepareResult = tfIdf
      .map(t => (t._1, Math.abs( MurmurHash3.stringHash(t._2) % (wordsCount * 2) ), t._3))

    println("Murmur " + prepareResult.collect())

    val res = tfIdf
      .map(t => (t._1, SparseVector.fromCOO(wordsCount * 2, (Math.abs( MurmurHash3.stringHash(t._2) % (wordsCount*2) ), t._3))))
      .groupBy(t => t._1)
      .reduce((t1, t2) =>
      (t1._1,
        SparseVector.fromCOO(t1._2.size + t2._2.size,t1._2.toSeq ++ t2._2.toSeq)))

    println()
    println("Result: " + res.collect())

    res
  }

  private def calculateTF(wordCounts: DataSet[(Int, String, Int)]): DataSet[(Int, String, Double)] = {
    val mostOftenWord = wordCounts
      //reduce to the count only
      .map(t => t._3)
      //take the biggest one
      .reduce((nr1, nr2) => if (nr1 > nr2) nr1 else nr2)
      .first(1)

    println("Most often = " + mostOftenWord.collect())
    val tf = wordCounts
      //combine with the most often word
      .crossWithTiny(mostOftenWord)
      //create one tuple for easier handling (docId, Word, count, most often word)
      .map(t => (t._1._1, t._1._2, t._1._3, t._2))
      //calculate the tf (docId, word, tf)
      .map(t => (t._1, t._2, t._3.asInstanceOf[Double] / t._4))
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
      .map( t => (t._1, log(totalNumberOfDocuments / t._2)))
    idf
  }
}

object StopWordParameter extends Parameter[List[String]] {
  override val defaultValue: Option[List[String]] = Some(List())
}