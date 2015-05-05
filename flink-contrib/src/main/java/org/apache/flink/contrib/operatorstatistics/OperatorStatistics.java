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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.HeavyHitter;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.LossyCounting;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.CountMinHeavyHitter;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.HeavyHitterMergeException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * Data structure that encapsulates statistical information of data that has only been processed by one pass
 * This statistical information is meant to help determine the distribution of the data that has been processed
 * in an operator so that we can determine if it is necessary to repartition the data
 *
 * The statistics to be gathered are configurable and represented by a {@link OperatorStatisticsConfig} object.
 *
 * The information encapsulated in this class is min, max, a structure enabling estimation of count distinct and a
 * structure holding the heavy hitters along with their frequency.
 *
 */
public class OperatorStatistics implements Serializable {

	OperatorStatisticsConfig config;

	Object min;
	Object max;
	long cardinality = 0;
	transient ICardinality countDistinct;
	transient HeavyHitter heavyHitter;

	public OperatorStatistics(OperatorStatisticsConfig config) {
		this.config = config;
		if (config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING)) {
			countDistinct = new LinearCounting(OperatorStatisticsConfig.COUNTD_BITMAP_SIZE);
		}
		if(config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.HYPERLOGLOG)){
			countDistinct = new HyperLogLog(OperatorStatisticsConfig.COUNTD_LOG2M);
		}
		if (config.heavyHitterAlgorithm.equals(OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING)){
			heavyHitter =
					new LossyCounting(OperatorStatisticsConfig.HEAVY_HITTER_FRACTION, OperatorStatisticsConfig.HEAVY_HITTER_ERROR);
		}
		if (config.heavyHitterAlgorithm.equals(OperatorStatisticsConfig.HeavyHitterAlgorithm.COUNT_MIN_SKETCH)){
			heavyHitter =
					new CountMinHeavyHitter(OperatorStatisticsConfig.HEAVY_HITTER_FRACTION,
											OperatorStatisticsConfig.HEAVY_HITTER_ERROR,
											OperatorStatisticsConfig.HEAVY_HITTER_CONFIDENCE,
											OperatorStatisticsConfig.HEAVY_HITTER_SEED);
		}
	}

	public void process(Object tupleObject){
		if (tupleObject instanceof Comparable) {
			if (config.collectMin && (min == null || ((Comparable) tupleObject).compareTo(min) < 0)) {
				min = tupleObject;
			}
			if (config.collectMax && (max == null || ((Comparable) tupleObject).compareTo(max) > 0)) {
				max = tupleObject;
			}
		}
		countDistinct.offer(tupleObject);
		heavyHitter.addObject(tupleObject);
		cardinality+=1;
	}

	public void merge(OperatorStatistics other) throws RuntimeException {

		if (this.config.collectMin && ((Comparable)this.min).compareTo(other.min) > 0 ) {
			this.min = other.min;
		}
		if (this.config.collectMax && ((Comparable)this.max).compareTo(other.max) < 0 ) {
			this.max = other.max;
		}

		try {
			this.heavyHitter.merge(other.heavyHitter);
		} catch (HeavyHitterMergeException e) {
			throw new RuntimeException("Error merging operator statistics",e);
		}

		try {
			ICardinality mergedCountDistinct = this.countDistinct.merge(new ICardinality[]{this.countDistinct,other.countDistinct});
			this.countDistinct = mergedCountDistinct;
		} catch (CardinalityMergeException e) {
			throw new RuntimeException("Error merging operator statistics",e);
		}
		this.cardinality+=other.cardinality;
	}

	public Object getMin() {
		return min;
	}

	public Object getMax() {
		return max;
	}

	public long estimateCountDistinct(){
		return countDistinct.cardinality();
	}

	public Map<Object,Long> getHeavyHitters(){
		return heavyHitter.getHeavyHitters();
	}

	@Override
	public String toString(){
		String out = "\nmax: "+this.max;
		out+="\nmin: "+this.min;
		out+="\ntotal cardinality: "+this.cardinality;
		out+="\ncount distinct estimate("+this.config.countDistinctAlgorithm+"): "+this.countDistinct.cardinality();
		out+="\nheavy hitters ("+this.config.heavyHitterAlgorithm +"):";
		out+="\n"+heavyHitter.toString();
		return out;
	}

	@Override
	public OperatorStatistics clone(){
		OperatorStatistics clone = new OperatorStatistics(this.config);
		clone.min = this.min;
		clone.max = this.max;
		clone.countDistinct = this.countDistinct;
		clone.heavyHitter = this.heavyHitter;
		return clone;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
/*		if (this.config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING)){
			out.write(countDistinct.getBytes());
			out.writeInt(((LinearCounting) countDistinct).getCount());
		}else{
			out.writeObject(countDistinct);
		}
		out.writeObject(heavyHitter);*/
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
	/*	try {
			local = (OperatorStatistics) in.readObject();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error when reading OperatorStatistics from input stream",e);
		}*/
	}

}