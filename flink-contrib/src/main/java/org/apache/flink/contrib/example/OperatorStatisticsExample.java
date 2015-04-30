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
package org.apache.flink.contrib.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple25;

/**
 * Use Case for OperatorStatistics
 */
public class OperatorStatisticsExample {

	public static void main(String[] args) throws Exception{

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		boolean[] includeFields = new boolean[58];
		for (int i=0;i<includeFields.length;i++){
			if (i<2 || (i>4 && i<35) || i==58){
				includeFields[i]=true;
			}else{
				includeFields[i]=false;
			}
		}

		DataSet events = env.readCsvFile("/home/tamara/Documents/Thesis/GDELT_dataset/20150422_export.CSV")
				.fieldDelimiter("\t").includeFields(includeFields).pojoType(GlobalEvent.class,
				"globalEventId","sqlDate","actor1Code","actor1Name","actor1CountryCode","actor1KnownGroupCode",
				"actor1EthnicCode","actor1Religion1","actor1Religion2","actor1Type1","actor1Type2","actor1Type3",
				"actor2Code","actor2Name","actor2CountryCode","actor2KnownGroupCode","actor2EthnicCode",
				"actor2Religion1","actor2Religion2","actor2Type1","actor2Type2","actor2Type3","isRootEvent",
				"eventCode","eventBaseCode","eventRootCode","quadClass","goldsteinScale","numMentions",
				"numSources","numArticles","avgTone","url");
		events.groupBy("actor1Name","actor2Name").getDataSet().print();
		JobExecutionResult result = env.execute();
	}

	public static class GlobalEvent {
		public int globalEventId = -1;
		public int sqlDate = -1;
		public String actor1Code = null;
		public String actor1Name = null;
		public String actor1CountryCode = null;
		public String actor1KnownGroupCode = null;
		public String actor1EthnicCode = null;
		public String actor1Religion1 = null;
		public String actor1Religion2 = null;
		public String actor1Type1 = null;
		public String actor1Type2 = null;
		public String actor1Type3 = null;
		public String actor2Code = null;
		public String actor2Name = null;
		public String actor2CountryCode = null;
		public String actor2KnownGroupCode = null;
		public String actor2EthnicCode = null;
		public String actor2Religion1 = null;
		public String actor2Religion2 = null;
		public String actor2Type1 = null;
		public String actor2Type2 = null;
		public String actor2Type3 = null;
		public int isRootEvent = -1;
		public String eventCode = null;
		public String eventBaseCode = null;
		public String eventRootCode = null;
		public int quadClass = -1;
		public Double goldsteinScale = null;
		public int numMentions = -1;
		public int numSources = -1;
		public int numArticles = -1;
		public Double avgTone = null;
		public String url = null;
	}

}
