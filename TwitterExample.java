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

package org.apache.flink.streaming.examples.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.connectors.twitter.TwitterSource.EndpointInitializer;
import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 *
 * <p>The input is a Tweet stream from a TwitterSource.
 *
 * <p>Usage: <code>Usage: TwitterExample [--output &lt;path&gt;]
 * [--twitter-source.consumerKey &lt;key&gt; --twitter-source.consumerSecret &lt;secret&gt; --twitter-source.token &lt;token&gt; --twitter-source.tokenSecret &lt;tokenSecret&gt;]</code><br>
 *
 * <p>If no parameters are provided, the program is run with default data from
 * {@link TwitterExampleData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>acquire external data,
 * <li>use in-line defined functions,
 * <li>handle flattened stream inputs.
 * </ul>
 */
public class TwitterExample {
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterExample.class);
	
	static final int DATASET_SIZE = 500;
	static final String POSITIVE_WORDS_FILE_PATH = "/home/student/git/flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/twitter/positive.txt";
	static final String NEGATIVE_WORDS_FILE_PATH = "/home/student/git/flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/twitter/negative.txt";
	static List<String> positiveWords = new ArrayList<String>();
	static List<String> negativeWords = new ArrayList<String>();
	static int globalScore = 0;
	static int tweetsCollected = 0;

	
	
	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		System.out.println("Usage: TwitterExample [--output <path>] " +
				"[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(params.getInt("parallelism", 1));
		
		TwitterExample.populateWordsLists();
				

		// get input data
		DataStream<String> streamSource;
		if (params.has(TwitterSource.CONSUMER_KEY) &&
				params.has(TwitterSource.CONSUMER_SECRET) &&
				params.has(TwitterSource.TOKEN) &&
				params.has(TwitterSource.TOKEN_SECRET)
				) {
			// Create and register a custom TwitterSource
			TwitterSource customSource = new TwitterSource(params.getProperties());
			customSource.setCustomEndpointInitializer((EndpointInitializer) new EndpointInitializerImpl(List.of("impeachment"))); // Only retrieve tweets that contain the word impeachment
			streamSource = env.addSource(customSource);
		} else {
			System.out.println("Executing TwitterStream example with default props.");
			System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
					"--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
			// get default test text data
			streamSource = env.fromElements(TwitterExampleData.TEXTS);
		}

		DataStream<Integer> tweets = streamSource
				.filter(new EnglishTweetsFilter())  // Filter tweets written in english
				.map(new TweetTextMapper())			// Map a Json tweet object to its' text
				.countWindowAll(DATASET_SIZE)		// Regroup tweets in 500 bunches 
				.apply(new MapTweetsToScore());		// Apply the custom window function to compute the sentiment score

		// emit result
		if (params.has("output")) {
			tweets.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			tweets.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Deserialize JSON from twitter source
	 *
	 * <p>Implements a string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean isEnglish = jsonNode.has("lang") && jsonNode.get("lang").asText().equals("en");
			boolean hasText = jsonNode.has("text");
			if (hasText && isEnglish) {
				// message of tweet
				StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

				// split the message
				while (tokenizer.hasMoreTokens()) {
					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

					if (!result.equals("")) {
						out.collect(new Tuple2<>(result, 1));
					}
				}
			}
		}
	}
	
	
	/**
	 * Filter tweets that are detected as english tweets
	 * @author student
	 *
	 */
	@SuppressWarnings("serial")
	public static class EnglishTweetsFilter implements FilterFunction<String> {
		private transient ObjectMapper jsonParser;
		@Override
		public boolean filter(String value) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			// Test whether the detected language is english
			return jsonNode.has("lang") && jsonNode.get("lang").asText().equals("en");
		}
		
	}
	/**
	 * Map a Json tweet object to it's text
	 * @author student
	 *
	 */
	@SuppressWarnings("serial")
	public static class TweetTextMapper implements MapFunction<String, String> {
		
		private transient ObjectMapper jsonParser;
		

		@Override
		public String map(String value) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasText = jsonNode.has("text");
			// If Json contains text property, then return it
			if (hasText) {
				return jsonNode.get("text").asText();
			}
			return "";
		}
		
	}
	/**
	 * Exercise 3.2
	 * A specific EndpointInitializer that allows to track specific terms in tweets
	 * @author student
	 *
	 */
	public static class EndpointInitializerImpl implements EndpointInitializer, Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		List<String> termsToTrack;		
	
		public EndpointInitializerImpl(List<String> termsToTrack) {
			this.termsToTrack = termsToTrack;
		}

		@Override
		public StreamingEndpoint createEndpoint() {
			StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
			endpoint.trackTerms(termsToTrack);
			return((StreamingEndpoint)endpoint);
		}
		
	}
	/**
	 * A static function to populate positive and negative words lists
	 * @throws IOException
	 */
	public static void populateWordsLists() throws IOException {
		File positiveWordsFile = new File(POSITIVE_WORDS_FILE_PATH);
		File negativeWordsFile = new File(NEGATIVE_WORDS_FILE_PATH);
		
		BufferedReader positiveWordsBr = new BufferedReader(new FileReader(positiveWordsFile));
		BufferedReader negativeWordsBr = new BufferedReader(new FileReader(negativeWordsFile));
		
		String line;
		while((line = positiveWordsBr.readLine()) != null) {
			if(!line.isEmpty() && !line.startsWith(";") && !line.startsWith("\n")) positiveWords.add(line.strip());
		}
		
		while((line = negativeWordsBr.readLine()) != null) {
			if(!line.isEmpty() && !line.startsWith(";") && !line.startsWith("\n")) negativeWords.add(line.strip());
		}
		positiveWordsBr.close();
		negativeWordsBr.close();
		
		LOG.error("Reading keywords done");
	}
	
	/**
	 * This function maps a tweet to its score value
	 * @author student
	 *
	 */
	@SuppressWarnings("serial")
	public class MapTextToScore implements MapFunction<String, Integer>{

		@Override
		public Integer map(String value) throws Exception {
			List<String> tokenizedTweet = List.of(value.split(" "));
			return tokenizedTweet.stream()
			.map(token -> {
				if(positiveWords.contains(token)) return 1;
				else if(negativeWords.contains(token)) return -1;
				else return 0;	
			})
			.reduce(Integer::sum)
			.orElseThrow(() -> new Exception("An error occured while computing the score"))
			;
		}
		
	}
	/**
	 * Compute the sentiment score for the current window
	 * @author student
	 *
	 */
	
	@SuppressWarnings("serial")
	public static class MapTweetsToScore implements AllWindowFunction<String, Integer, GlobalWindow>{
		
		@Override
		public void apply(GlobalWindow window, Iterable<String> values, Collector<Integer> out) throws Exception {
			
			
			
			int windowSum = StreamSupport.stream(values.spliterator(), false)
					.flatMap(tweet -> { return 
							List.of(tweet.replaceAll("[\n\'\",;.:#@]", "") // Replace punctuation and newline sequence by empty char
									.toLowerCase() 
									.split(" ")) // Create a list of tokens by splitting the string by whitespaces 
									.stream();}) // Output a Stream of all tokens contained in the dataset
					.map(token -> {
						if(positiveWords.contains(token)) return 1;
						else if (negativeWords.contains(token)) return -1;
						else return 0;
					})
					.mapToInt(i->i)
					.sum();
			globalScore +=windowSum;
			tweetsCollected += values.spliterator().estimateSize();
			System.out.println("For " + tweetsCollected + " tweets collected | global score = " + globalScore);
			out.collect(windowSum);	
		}
	

	}
}
