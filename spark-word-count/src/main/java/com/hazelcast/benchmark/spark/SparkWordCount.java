package com.hazelcast.benchmark.spark;

import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by can on 23/02/2016.
 */
public class SparkWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Pattern space = Pattern.compile(" ");

        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaRDD<String> words = textFile.flatMap(s -> Arrays.asList(space.split(s)).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);


        System.out.println("Starting task");
        long t = System.currentTimeMillis();
        counts.saveAsTextFile(args[1] + "_" + t);
        System.out.println("Time=" + (System.currentTimeMillis() - t));

    }
}
