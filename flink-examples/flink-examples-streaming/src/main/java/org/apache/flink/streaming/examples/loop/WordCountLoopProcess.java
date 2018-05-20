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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides the default data sets used for the WordCountLoopProcess example program.
 * The default data sets are used, if no parameters are given to the program.
 */
public class WordCountLoopProcess {
	public static final String[] WORDS = new String[]{
		"To be, or not to be,--that is the question:--",
		"Whether 'tis nobler in the mind to suffer",
		"The slings and arrows of outrageous fortune",
		"Or to take arms against a sea of troubles,",
		"And by opposing end them?--To die,--to sleep,--",
		"No more; and by a sleep to say we end",
		"The heartache, and the thousand natural shocks",
		"That flesh is heir to,--'tis a consummation",
		"Devoutly to be wish'd. To die,--to sleep;--",
		"To sleep! perchance to dream:--ay, there's the rub;",
		"For in that sleep of death what dreams may come,",
		"When we have shuffled off this mortal coil,",
		"Must give us pause: there's the respect",
		"That makes calamity of so long life;",
		"For who would bear the whips and scorns of time,",
		"The oppressor's wrong, the proud man's contumely,",
		"The pangs of despis'd love, the law's delay,",
		"The insolence of office, and the spurns",
		"That patient merit of the unworthy takes,",
		"When he himself might his quietus make",
		"With a bare bodkin? who would these fardels bear,",
		"To grunt and sweat under a weary life,",
		"But that the dread of something after death,--",
		"The undiscover'd country, from whose bourn",
		"No traveller returns,--puzzles the will,",
		"And makes us rather bear those ills we have",
		"Than fly to others that we know not of?",
		"Thus conscience does make cowards of us all;",
		"And thus the native hue of resolution",
		"Is sicklied o'er with the pale cast of thought;",
		"And enterprises of great pith and moment,",
		"With this regard, their currents turn awry,",
		"And lose the name of action.--Soft you now!",
		"The fair Ophelia!--Nymph, in thy orisons",
		"Be all my sins remember'd."
	};
	private static int maxCount = 100;
	private static int rate = 1;
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

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
			System.out.println("Executing WordCountLoopProcess example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.addSource(new FixedIntervalTextSource());
		}
		if (params.has("rate")) {
			rate = Integer.parseInt(params.get("rate"));
		} else {
			System.out.println("Emitting WordCountLoopProcess example with default rate 1s.");
			System.out.println("Use --rate to specify rate.");
		}
		if (params.has("maxCount")) {
			maxCount = Integer.parseInt(params.get("maxCount"));
		} else {
			System.out.println("Emitting WordCountLoopProcess example with default max count 100.");
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

		int wmkMaxOutOrder = 0;
		if (params.has("wmkMaxOutOrder")) {
			wmkMaxOutOrder = Integer.parseInt(params.get("wmkMaxOutOrder"));
		} else {
			wmkMaxOutOrder = 10;
			System.out.println("watermark max out-order time with default 10s.");
			System.out.println("Use --wmkMaxOutOrder to specify watermark max out-order time.");
		}

		int tumbWinTime = 0;
		if (params.has("tumbWinTime")) {
			tumbWinTime = Integer.parseInt(params.get("tumbWinTime"));
		} else {
			tumbWinTime = 3;
			System.out.println("tumbing Window time with default 3s.");
			System.out.println("Use --tumbWinTime to specify tumbing Window time.");
		}

		env.enableCheckpointing(chkInterval * 1000L);
		env.getCheckpointConfig().setCheckpointTimeout(chkTimeout * 1000L);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(chkMinPause * 1000L);
		RocksDBStateBackend backend =
			new RocksDBStateBackend(chkDir, true);
		backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		env.setStateBackend(backend);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //turn on watermark

		DataStream<Tuple2<String, Long>> watermarkStream = text.flatMap(new Tokenizer()).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(wmkMaxOutOrder * 1000));

		DataStream<Tuple6<String, Integer, String, String, String, String>> window = watermarkStream.keyBy(1).
			window(TumblingEventTimeWindows.of(Time.seconds(tumbWinTime * 1000)))
			.apply(new MyWindowFunction());

		// emit result
		if (params.has("output")) {
			window.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			window.print();
		}
		// execute program
		env.execute("Streaming WordCountLoopProcess");
	}

	public static <T> List<T> convertIterableToList(Iterable<T> iterable) {
		Iterator<T> iterator = iterable.iterator();
		List<T> convertList = new ArrayList<T>();
		while (iterator.hasNext()) {
			convertList.add(iterator.next());
		}
		return convertList;
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)"
	 * ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class MyAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
		private long currentMaxTimestamp = 0L;
		private long maxOutOfOrderness = 10000L; //enable out-order time 10s
		private Watermark watermark = null;

		public MyAssignerWithPeriodicWatermarks() {
		}

		public MyAssignerWithPeriodicWatermarks(long maxOutOfOrderness) {
			this.maxOutOfOrderness = maxOutOfOrderness;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
			return watermark;
		}

		@Override
		public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
			long timestamp = element.f1;
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			if (watermark != null) {
				System.out.println("timestamp:" + element.f0 + "," + element.f1 + "|" + format.format(element.f1)
					+ "," + currentMaxTimestamp + "|" + format.format(currentMaxTimestamp) + "," + watermark.toString());
			} else {
				System.out.println("watermark is not init");
			}
			return timestamp;
		}
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)"
	 * ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class MyWindowFunction implements WindowFunction<Tuple2<String, Long>,
		Tuple6<String, Integer, String, String, String, String>, Tuple, TimeWindow> {
		@Override
		public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple6<String, Integer, String, String, String, String>> out) throws Exception {
			List<Tuple2<String, Long>> inputList = convertIterableToList(input);
			List<Tuple2<String, Long>> inputSorted = inputList.stream().sorted((o1, o2) -> o1.f1.compareTo(o2.f1)).collect(Collectors.toList());
			out.collect(new Tuple6(tuple.toString(), inputSorted.size(), format.format(inputSorted.get(0).f1),
				format.format(inputSorted.get(inputSorted.size() - 1).f1), format.format(window.getStart()), format.format(window.getEnd())));
		}
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
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				long eventTime = System.currentTimeMillis();
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Long>(token, eventTime));
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
				for (String tuple : WORDS) {
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
