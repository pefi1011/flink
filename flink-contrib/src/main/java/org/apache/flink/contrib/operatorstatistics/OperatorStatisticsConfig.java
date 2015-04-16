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

import java.io.Serializable;

/**
 * Configures the behavior of an {@link OperatorStatistics} instance.
 *
 * Sets the parameters that determine the accuracy of the count distinct and heavy hitter sketches
 *
 * Defines the statistics to be collected. A boolean field indicates whether a given statistic should be collected or not
 *
 * Encapsulates an enum indicating which sketch should be used for count distinct and another indicating which sketch
 * should be used for detecting heavy hitters.
 */

public class OperatorStatisticsConfig implements Serializable {

	public static final int COUNTD_BITMAP_SIZE = 1000000;
	public static final int COUNTD_LOG2M = 20;

	public static final int HEAVY_HITTER_SEED = 121311332;
	public static final double HEAVY_HITTER_CONFIDENCE = 0.99;
	public static final double HEAVY_HITTER_FRACTION = 0.05;
	public static final double HEAVY_HITTER_ERROR = 0.005;

	public boolean collectMin;
	public boolean collectMax;
	public boolean collectCountDistinct;
	public boolean collectHeavyHitters;
	public CountDistinctAlgorithm countDistinctAlgorithm;
	public HeavyHitterAlgorithm heavyHitterAlgorithm;

	public OperatorStatisticsConfig(){
		this.collectMin = true;
		this.collectMax = true;
		this.collectCountDistinct = true;
		this.collectHeavyHitters = true;
		this.countDistinctAlgorithm = CountDistinctAlgorithm.HYPERLOGLOG;
		this.heavyHitterAlgorithm = HeavyHitterAlgorithm.LOSSY_COUNTING;
	}

	public OperatorStatisticsConfig(CountDistinctAlgorithm countDistinct, HeavyHitterAlgorithm heavyHitter) {
		this.collectMin = true;
		this.collectMax = true;
		this.collectCountDistinct = true;
		this.collectHeavyHitters = true;
		this.countDistinctAlgorithm = countDistinct;
		this.heavyHitterAlgorithm = heavyHitter;
	}

	public enum CountDistinctAlgorithm {

		LINEAR_COUNTING,
		HYPERLOGLOG;

	}

	public enum HeavyHitterAlgorithm{
		LOSSY_COUNTING,
		COUNT_MIN_SKETCH;
	}

}
