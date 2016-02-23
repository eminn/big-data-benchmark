package com.hazelcast.benchmark.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by can on 23/02/2016.
 */
public class SparkWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[" + Runtime.getRuntime().availableProcessors() + "]")
                .setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });

        System.out.println("Starting task");
        long t = System.currentTimeMillis();
        counts.saveAsTextFile(args[1]);
        System.out.println("Time=" + (System.currentTimeMillis() - t));
    }
}
