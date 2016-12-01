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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

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

        Pattern space = Pattern.compile(" ");
        if (!parseParameters(args)) {
            return;
        }

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get input data
        DataSet<String> text = getTextDataStream(env);

        DataSet<Tuple2<String, Integer>> counts =
                // normalize and split each line
                text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                                // emit the pairs with non-zero-length words
                                for (String s : space.split(line)) {
                                    out.collect(new Tuple2<>(s, 1));
                                }
                            }
                        })
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // emit result
        counts.writeAsCsv(outputPath + "_" + System.currentTimeMillis());
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

    private static DataSet<String> getTextDataStream(ExecutionEnvironment env) {
        return env.readTextFile(textPath);
    }
}