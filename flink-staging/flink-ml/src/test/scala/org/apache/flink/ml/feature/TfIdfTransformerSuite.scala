package org.apache.flink.ml.feature

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}
import org.apache.flink.api.scala._

/**
 * @author Ronny Bräunlich
 */
class TfIdfTransformerSuite extends FlatSpec with Matchers with FlinkTestBase{

  behavior of "the tf idf transformer implementation"

  it should "calculate four times zero for four words in only one document" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = "This is a test".split(" ").toSeq
    val documentKey = 1

    val inputDs = env.fromCollection(Seq((documentKey, input)))
    val transformer = new TfIdfTransformer()

    val result = transformer.transform(inputDs)
    val resultColl = result.collect()

    resultColl.length should be (1)
    resultColl(0)._1 should be (documentKey)
    resultColl(0)._2.size should be (4)

    for( x <- resultColl(0)._2){
      x._2 should be (0.0)
    }

  }

}

