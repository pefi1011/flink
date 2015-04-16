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

package org.apache.flink.contrib.operatorstatistics;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

public class OperatorStatsAccumulatorsTest extends AbstractTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorStatsAccumulatorsTest.class);

	private static final String ACCUMULATOR_NAME = "op-stats";

	public OperatorStatsAccumulatorsTest(){
		super(new Configuration());
	}

	@Test
	public void testAccumulator() throws Exception {

		String input = "";

		Random rand = new Random();

		for (int i = 1; i < 1000; i++) {
			if(rand.nextDouble()<0.2){
				input+=String.valueOf(rand.nextInt(5))+"\n";
			}else{
				input+=String.valueOf(rand.nextInt(100))+"\n";
			}
		}

		String inputFile = createTempFile("datapoints.txt", input);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readTextFile(inputFile).
				flatMap(new StringToInt()).
				output(new DiscardingOutputFormat<Tuple1<Integer>>());

		JobExecutionResult result = env.execute();

		OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
		LOG.debug("Global Stats");
		LOG.debug(globalStats.toString());

		OperatorStatistics merged = null;

		Map<String,Object> accResults = result.getAllAccumulatorResults();
		for (String accumulatorName:accResults.keySet()){
			if (accumulatorName.contains(ACCUMULATOR_NAME+"-")){
				OperatorStatistics localStats = (OperatorStatistics) accResults.get(accumulatorName);
				if (merged == null){
					merged = localStats.clone();
				}else {
					merged.merge(localStats);
				}
				LOG.debug("Local Stats: " + accumulatorName);
				LOG.debug(localStats.toString());
			}
		}

		Assert.assertEquals(globalStats.cardinality,999);
		Assert.assertEquals(globalStats.estimateCountDistinct(),100);
		Assert.assertTrue(globalStats.getHeavyHitters().size()>0 && globalStats.getHeavyHitters().size()<=5);
		Assert.assertEquals(merged.getMin(),globalStats.getMin());
		Assert.assertEquals(merged.getMax(),globalStats.getMax());
		Assert.assertEquals(merged.estimateCountDistinct(),globalStats.estimateCountDistinct());
		Assert.assertEquals(merged.getHeavyHitters().size(),globalStats.getHeavyHitters().size());

	}

	public static class StringToInt extends RichFlatMapFunction<String, Tuple1<Integer>> {

		// Is instantiated later since the runtime context is not yet initialized
		private Accumulator<Object, Serializable> globalAccumulator;
		private Accumulator<Object,Serializable>[] localAccumulators;

		@Override
		public void open(Configuration parameters) {
			// Add globalAccumulator using convenience function

			globalAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
			if (globalAccumulator==null){
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, new OperatorStatisticsAccumulator());
				globalAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
			}
			int subTasks = getRuntimeContext().getNumberOfParallelSubtasks();

			if (localAccumulators==null){
				localAccumulators = new Accumulator[subTasks];
			}

			int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
			if (localAccumulators[subTaskIndex]==null){
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex, new OperatorStatisticsAccumulator());
				localAccumulators[subTaskIndex] = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex);
			}
		}

		@Override
		public void flatMap(String value, Collector<Tuple1<Integer>> out) throws Exception {
			int intValue;
			try {
				intValue = Integer.parseInt(value);
				globalAccumulator.add(intValue);
				localAccumulators[getRuntimeContext().getIndexOfThisSubtask()].add(intValue);
				out.collect(new Tuple1(intValue));
			} catch (NumberFormatException ex) {
			}
		}
	}
}
