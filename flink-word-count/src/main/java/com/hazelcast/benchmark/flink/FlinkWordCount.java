/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.benchmark.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Implements the streaming "WordCount" program that computes a simple word occurrences
 * over text files. 
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 *
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a compact Flink Streaming program with Java 8 Lambda Expressions.
 * </ul>
 *
 */
public class FlinkWordCount {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> text = getTextDataStream(env);

        DataStream<Tuple2<String, Integer>> counts =
                // normalize and split each line
                text.map(line -> line.split("\\W+"))
                        // convert splitted line in pairs (2-tuples) containing: (word,1)
                        .flatMap(new FlatMapFunction<String[], Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String[] tokens, Collector<Tuple2<String, Integer>> out) throws Exception {
                                // emit the pairs with non-zero-length words
                                Arrays.stream(tokens)
                                        .filter(t -> t.length() > 0)
                                        .forEach(t -> out.collect(new Tuple2<>(t, 1)));
                            }
                        })
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .sum(1);

        // emit result
        counts.writeAsCsv(outputPath);
        // execute program
        long t =  System.currentTimeMillis();
        env.execute("Streaming WordCount Example");
        System.out.println("Time=" + (System.currentTimeMillis() - t));
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static String textPath;
    private static String outputPath;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            // parse input arguments
            if (args.length == 2) {
                textPath = args[0];
                outputPath = args[1];
            } else {
                System.err.println("Usage: WordCount <text path> <result path>");
                return false;
            }
        } else {
            System.out.println("  Provide parameters to read input data from a file.");
            System.out.println("  Usage: WordCount <text path> <result path>");
            return false;
        }
        return true;
    }

    private static DataStream<String> getTextDataStream(StreamExecutionEnvironment env) {
        return env.readTextFile(textPath);
    }
}