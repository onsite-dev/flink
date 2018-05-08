package org.apache.flink.streaming.examples.loop;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Provides the default data sets used for the WordCount example program.
 * The default data sets are used, if no parameters are given to the program.
 */
public class WordCount {
	private static int maxCount = 100;
	private static int rate = 1;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.addSource(new FixedIntervalTextSource());
		}
		if (params.has("rate")) {
			rate = Integer.parseInt(params.get("rate"));
		} else {
			System.out.println("Emitting WordCount example with default rate 1s.");
			System.out.println("Use --rate to specify rate.");
		}
		if (params.has("maxCount")) {
			maxCount = Integer.parseInt(params.get("maxCount"));
		} else {
			System.out.println("Emitting WordCount example with default max count 100.");
			System.out.println("Use --maxCount to specify max count of emitting.");
		}
		int chkInterval = 0;
		if (params.has("chkInterval")) {
			chkInterval = Integer.parseInt(params.get("chkInterval"));
		} else {
			chkInterval = 10;
			System.out.println("checkpoint interval with default 10s.");
			System.out.println("Use --chkInterval to specify checkpoint interval.");
		}

		int chkTimeout = 0;
		if (params.has("chkTimeout")) {
			chkTimeout = Integer.parseInt(params.get("chkTimeout"));
		} else {
			chkTimeout = 5;
			System.out.println("checkpoint timeout with default 5s.");
			System.out.println("Use --chkTimeout to specify checkpoint timeout.");
		}

		int chkMinPause = 0;
		if (params.has("chkMinPause")) {
			chkMinPause = Integer.parseInt(params.get("chkMinPause"));
		} else {
			chkMinPause = 5;
			System.out.println("checkpoint min pause with default 5s.");
			System.out.println("Use --chkMinPause to specify checkpoint min pause.");
		}

		String chkDir = "";
		if (params.has("chkDir")) {
			chkDir = params.get("chkDir");
		} else {
			chkDir = "hdfs:///flink/chk";
			System.out.println("checkpoint chkDir with default hdfs:///flink/chk.");
			System.out.println("Use --chkDir to specify checkpoint dir.");
		}

		env.enableCheckpointing(chkInterval * 1000L);
		env.getCheckpointConfig().setCheckpointTimeout(chkTimeout * 1000L);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(chkMinPause * 1000L);
		RocksDBStateBackend backend =
			new RocksDBStateBackend(chkDir, true);
		backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		env.setStateBackend(backend);

		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field
				// "1"
				.keyBy(0).sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

		// execute program
		env.execute("Streaming WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)"
	 * ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	private static class FixedIntervalTextSource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		private int counter = 0;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {

			while (isRunning && counter < maxCount) {
				for (String tuple : WordCountData.WORDS) {
					ctx.collect(tuple);
				}
				counter++;
				Thread.sleep(rate * 1000L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
