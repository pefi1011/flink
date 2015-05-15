package de.tu_berlin.impro3.flink

import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.api.common.operators.Order

/**
 * @author Ronny Bräunlich
 */
class TfIdfTransformer extends Transformer[(Int, Seq[String]),(Int, SparseVector)] {

  override def transform(input: DataSet[(Int,Seq[String])], transformParameters: ParameterMap): DataSet[(Int,SparseVector)] = {
   val groupedWordCounts = input
      //count the words
      .flatMap(t  => {
        t._2.map(s => (t._1, s, 1))
      })
      .reduce((tup1, tup2) => (tup1._1, tup1._2, tup1._3 + tup2._3))
      .groupBy(t => t._1)
      .sortGroup(t => t._3, Order.DESCENDING)
    val mostOftenOccurringWord = groupedWordCounts.first(1).map(t => t._3).reduce((a,b) => if (a > b) a else b)
    null
 /* input.
    map {
    x => val filtered = x._2.filter{ y => !transformParameters.apply(StopWordParameter).contains(y) }
      (x._1, filtered)
    }
    .map {
      x =>
        val map = new Map()
        x._2.map(y => map.put(y -> 1))
        (x._1, x._2.z)
    })
    return null*/
   //the sequence are the documents and we have to transform them into bag of words
  //TF IDF across the corpus
  }
}//transformParameters.apply(StopWordParameter).contains(y)

object StopWordParameter extends Parameter[List[String]]{
  override val defaultValue: Option[List[String]] = Some(List())
}