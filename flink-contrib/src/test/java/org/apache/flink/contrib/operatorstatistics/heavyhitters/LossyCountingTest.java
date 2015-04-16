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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/*
* Test the structure implemented for Lossy Counting
*/

public class LossyCountingTest {

	private static final Logger LOG = LoggerFactory.getLogger(LossyCountingTest.class);

	static final double fraction = 0.05;
	static final double error = 0.005;
	static final int seed = 7362181;
	static final Random r = new Random(seed);

	@Test
	public void testAccuracy() {

		int numItems = 1000000;
		long frequency = (int)Math.ceil(numItems* fraction);
		long minFrequency = (int)Math.ceil(numItems* (fraction-error));

		int[] xs = new int[numItems];
		int maxScale = 20;

		for (int i = 0; i < numItems; i++) {
			double p = r.nextDouble();
			if (p<0.2){
				xs[i] = r.nextInt(5);
			}else {
				int scale = r.nextInt(maxScale);
				xs[i] = r.nextInt(1 << scale);
			}
		}

		LossyCounting lossyCounting = new LossyCounting(fraction,error);

		for (int x : xs) {
			lossyCounting.addObject(x);
		}

		long[] actualFreq = new long[1 << maxScale];
		for (int x : xs) {
			actualFreq[x]++;
		}

		LOG.debug("Size of heavy hitters: "+lossyCounting.heavyHitters.size());
		LOG.debug(lossyCounting.toString());

		for (int i = 0; i < actualFreq.length; ++i) {
			if (actualFreq[i]>=frequency) {
				assertTrue("All items with freq. > s.n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Expected freq."+frequency, lossyCounting.getHeavyHitters().containsKey(i));
			}
			if (lossyCounting.getHeavyHitters().containsKey(i)){
				assertTrue("no item with freq. < (s-e).n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Min freq."+ minFrequency, actualFreq[i]>=minFrequency);
				assertTrue("the estimated freq. underestimates the true freq. by < e.n. Real freq. " + actualFreq[i] + " Lower bound "+lossyCounting.getHeavyHitters().get(i),
						Math.abs(lossyCounting.getHeavyHitters().get(i)-actualFreq[i]) < error*numItems);
			}
		}
	}

	@Test
	public void merge() throws HeavyHitterMergeException {
		int numToMerge = 5;
		int cardinality = 10000;
		int maxScale = 20;

		LossyCounting baseline = new LossyCounting(fraction,error);

		LossyCounting merged = new LossyCounting(fraction,error);

		LossyCounting[] sketches = new LossyCounting[numToMerge];
		for (int i = 0; i < numToMerge; i++) {
			sketches[i] = new LossyCounting(fraction,error/2);
			for (int j = 0; j < cardinality; j++) {
				double p = r.nextDouble();
				if (p<0.2){
					int val = r.nextInt(5);
					sketches[i].addObject(val);
					baseline.addObject(val);
				}else {
					int scale = r.nextInt(maxScale);
					int val = r.nextInt(1 << scale);
					sketches[i].addObject(val);
					baseline.addObject(val);
				}
			}
			merged.merge(sketches[i]);
		}

		LOG.debug("\nMERGED\n"+merged.toString());
		LOG.debug("\nBASELINE\n"+baseline.toString());

		for (Map.Entry<Object, Long> entry : baseline.getHeavyHitters().entrySet()){
			assertTrue("Frequent item in baseline is not frequent in merged: " + entry.getKey(), merged.heavyHitters.containsKey(entry.getKey()));
		}
	}
}
