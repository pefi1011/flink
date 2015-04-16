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

package org.apache.flink.contrib.operatorstatistics.heavyhitters;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.CountMinHeavyHitter;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the structure built to track heavy hitters using the count min sketch from
 * {@link com.clearspring.analytics.stream.frequency.CountMinSketch}
 */
public class CountMinHeavyHitterTest {

	@Test
	public void testAccuracy() {
		int seed = 7364181;
		Random r = new Random(seed);
		int numItems = 1000000;
		double fraction = 0.2;
		long frequency = (int)Math.ceil(numItems*fraction);

		int[] xs = new int[numItems];
		int maxScale = 20;

		for (int i = 0; i < numItems; i++) {
			double p = r.nextDouble();
			if (p<0.2){
				xs[i] = 1;
			}else if (p<0.4) {
				xs[i] = 12;
			}else {
				int scale = r.nextInt(maxScale);
				xs[i] = r.nextInt(1 << scale);
			}
		}

		double epsOfTotalCount = 0.0001;
		double confidence = 0.99;

		CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
		CountMinHeavyHitter cmTopK = new CountMinHeavyHitter(sketch,fraction);

		for (int x : xs) {
			cmTopK.addLong(x, 1);
		}

		long[] actualFreq = new long[1 << maxScale];
		for (int x : xs) {
			actualFreq[x]++;
		}

		for (int i = 0; i < actualFreq.length; ++i) {
			if (actualFreq[i]>=frequency){
				assertTrue("Frequent item not found: item " + i + ", frequency " + actualFreq[i], cmTopK.heavyHitters.containsKey((long)i));
			}else{
				assertTrue("False Positive: " + i + ", frequency " + actualFreq[i] + " (min expected frequency "+frequency+")", !cmTopK.heavyHitters.containsKey((long)i));
			}
		}
	}

	@Test
	public void merge() throws CountMinHeavyHitter.CMHeavyHitterMergeException {
		int numToMerge = 5;
		int cardinality = 1000000;

		double epsOfTotalCount = 0.0001;

		double confidence = 0.99;
		int seed = 7364181;
		double fraction = 0.2;

		int maxScale = 20;
		Random r = new Random();

		CountMinSketch sketchBaseline = new CountMinSketch(epsOfTotalCount, confidence, seed);
		CountMinHeavyHitter baseline = new CountMinHeavyHitter(sketchBaseline,fraction);
		CountMinHeavyHitter merged = null;

		CountMinHeavyHitter[] sketches = new CountMinHeavyHitter[numToMerge];
		for (int i = 0; i < numToMerge; i++) {
			CountMinSketch cms = new CountMinSketch(epsOfTotalCount, confidence, seed);
			sketches[i] = new CountMinHeavyHitter(cms, fraction);
			for (int j = 0; j < cardinality; j++) {
				double p = r.nextDouble();
				if (p<0.2){
					sketches[i].addLong(1 * i + 1, 1);
					baseline.addLong(1 * i + 1, 1);
				}else if (p<0.4) {
					sketches[i].addLong(50 * i + 1, 1);
					baseline.addLong(50 * i + 1, 1);
				}else {
					int scale = r.nextInt(maxScale);
					int val = r.nextInt(1 << scale);
					sketches[i].addLong(val, 1);
					baseline.addLong(val, 1);
				}
			}
			if (i==0){
				merged = sketches[0];
			}else{
				merged.merge(sketches[i]);
			}
		}

		for (Map.Entry<Object, Long> entry : baseline.heavyHitters.entrySet()){
			assertTrue("Frequent item in baseline is not frequent in merged: " + entry.getKey(), merged.heavyHitters.containsKey(entry.getKey()));
			assertEquals(entry.getValue(), merged.heavyHitters.get(entry.getKey()));
		}
	}

}
