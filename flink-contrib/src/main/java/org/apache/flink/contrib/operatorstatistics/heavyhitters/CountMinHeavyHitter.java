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

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/*
 * This class tracks heavy hitters using the {@link com.clearspring.analytics.stream.frequency.CountMinSketch} structure
 * to estimate frequencies.
 */
public class CountMinHeavyHitter implements HeavyHitter {

	CountMinSketch countMinSketch;
	HashMap<Object,Long> heavyHitters;
	double fraction;
	long cardinality;

	public CountMinHeavyHitter(double fraction, double error, double confidence, int seed){
		this.countMinSketch = new CountMinSketch(error,confidence,seed);
		this.cardinality = 0;
		this.fraction = fraction;
		this.heavyHitters = new HashMap<Object,Long>();
	}

	public CountMinHeavyHitter(CountMinSketch countMinSketch, double fraction){
		this.countMinSketch = countMinSketch;
		this.cardinality = 0;
		this.fraction = fraction;
		this.heavyHitters = new HashMap<Object,Long>();
	}

	public void addLong(long item, long count) {
		countMinSketch.add(item,count);
		cardinality +=count;
		updateHeavyHitters(item);
	}

	public void addString(String item, long count) {
		countMinSketch.add(item,count);
		cardinality +=count;
		updateHeavyHitters(item);
	}

	@Override
	public void addObject(Object o) {
		long objectHash = MurmurHash.hash(o);
		countMinSketch.add(objectHash, 1);
		cardinality +=1;
		updateHeavyHitters(objectHash);
	}

	private void updateHeavyHitters(long item){
		long minFrequency = (long)Math.ceil(cardinality * fraction);
		long estimateCount = countMinSketch.estimateCount(item);
		if (estimateCount >= minFrequency){
			heavyHitters.put(item, estimateCount);
		}
		removeNonFrequent(minFrequency);
	}

	private void updateHeavyHitters(String item){
		long minFrequency = (long)Math.ceil(cardinality * fraction);
		long estimateCount = countMinSketch.estimateCount(item);
		if (estimateCount >= minFrequency){
			heavyHitters.put(item, estimateCount);
		}
		removeNonFrequent(minFrequency);
	}

	private void removeNonFrequent(long minFrequency){
		ArrayList<Object> nonFrequentKeys = new ArrayList<Object>();
		for (Map.Entry<Object, Long> entry : heavyHitters.entrySet()){
			if(entry.getValue()<minFrequency){
				nonFrequentKeys.add(entry.getKey());
			}
		}
		for (Object o:nonFrequentKeys){
			heavyHitters.remove(o);
		}
	}

	public void merge(HeavyHitter toMerge) throws CMHeavyHitterMergeException {

		try {
			CountMinHeavyHitter cmToMerge = (CountMinHeavyHitter)toMerge;
			this.countMinSketch = CountMinSketch.merge(this.countMinSketch, cmToMerge.countMinSketch);

			if (this.fraction != cmToMerge.fraction) {
				throw new CMHeavyHitterMergeException("Frequency expectation cannot be merged");
			}

			for (Map.Entry<Object, Long> entry : cmToMerge.heavyHitters.entrySet()) {
				if (this.heavyHitters.containsKey(entry.getKey())){
					this.heavyHitters.put(entry.getKey(),this.countMinSketch.estimateCount((Long)entry.getKey()));
				}else{
					this.heavyHitters.put(entry.getKey(),entry.getValue());
				}
			}

			cardinality+=cmToMerge.cardinality;
			long minFrequency = (long) Math.ceil(cardinality * fraction);
			this.removeNonFrequent(minFrequency);

		}catch (ClassCastException ex){
			throw new CMHeavyHitterMergeException("Both heavy hitter objects must belong to the same class");
		}catch (Exception ex){
			throw new CMHeavyHitterMergeException("Cannot merge count min sketches: "+ex.getMessage());
		}
	}


	@Override
	public HashMap getHeavyHitters() {
		return heavyHitters;
	}

	protected static class CMHeavyHitterMergeException extends HeavyHitterMergeException {
		public CMHeavyHitterMergeException(String message) {
			super(message);
		}
	}

	@Override
	public String toString(){
		String out = "";
		for (Map.Entry<Object, Long> entry : heavyHitters.entrySet()){
			out += entry.getKey().toString() + " -> estimated freq. " + entry.getValue() + "\n";
		}
		return out;
	}

}
