package com.hazelcast.benchmark.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.connector.hadoop.HdfsReader;
import com.hazelcast.connector.hadoop.HdfsWriter;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.JetEngineConfig;
import com.hazelcast.jet.Vertex;
import com.hazelcast.logging.ILogger;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import static com.hazelcast.jet.impl.Util.uncheckedGet;

public class JetWordCount {

    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static HazelcastInstance instance;


    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            Config config = new Config();
            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true);
            join.getTcpIpConfig().addMember("server1");
            join.getTcpIpConfig().addMember("server2");
            join.getTcpIpConfig().addMember("server3");
            join.getTcpIpConfig().addMember("server4");
            join.getTcpIpConfig().addMember("server5");
            join.getTcpIpConfig().addMember("server6");
            join.getTcpIpConfig().addMember("server7");
            join.getTcpIpConfig().addMember("server8");
            join.getTcpIpConfig().addMember("server9");
            Hazelcast.newHazelcastInstance(config);
            return;
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("server1");
        clientConfig.getNetworkConfig().addAddress("server2");
        clientConfig.getNetworkConfig().addAddress("server3");
        clientConfig.getNetworkConfig().addAddress("server4");
        clientConfig.getNetworkConfig().addAddress("server5");
        clientConfig.getNetworkConfig().addAddress("server6");
        clientConfig.getNetworkConfig().addAddress("server7");
        clientConfig.getNetworkConfig().addAddress("server8");
        clientConfig.getNetworkConfig().addAddress("server9");

        instance = HazelcastClient.newHazelcastClient(clientConfig);


        System.out.println("Press any key to start");
        System.in.read();
        JetEngineConfig config = new JetEngineConfig().setParallelism(PARALLELISM);
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine", config);


        String inputPath = args[0];
        String outputPath = args[1] + "_" + System.currentTimeMillis();

        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", HdfsReader.supplier(inputPath));
        Vertex generator = new Vertex("generator", Generator::new);
        Vertex accumulator = new Vertex("accumulator", Combiner::new);
        Vertex combiner = new Vertex("combiner", Combiner::new);
        Vertex consumer = new Vertex("consumer", HdfsWriter.supplier(outputPath));

        dag
                .addVertex(producer)
                .addVertex(generator)
                .addVertex(accumulator)
                .addVertex(combiner)
                .addVertex(consumer)
                .addEdge(new Edge(producer, generator))
                .addEdge(new Edge(generator, accumulator)
                        .partitioned((item, n) -> Math.abs(((Map.Entry) item).getKey().hashCode()) % n))
                .addEdge(new Edge(accumulator, combiner)
                        .distributed()
                        .partitioned(item -> ((Map.Entry) item).getKey()))
                .addEdge(new Edge(combiner, consumer));

        benchmark("jet", () -> {
            uncheckedGet(jetEngine.newJob(dag).execute());
        });

    }


    static void benchmark(String label, Runnable run) {
        List<Long> times = new ArrayList<>();
        long testStart = System.currentTimeMillis();
        int warmupCount = 0;
        boolean warmupEnded = false;
        ILogger logger = instance.getLoggingService().getLogger(JetWordCount.class);
        logger.info("Starting test..");
        logger.info("Warming up...");
//        while (true) {
        long start = System.currentTimeMillis();
        run.run();
        long end = System.currentTimeMillis();
        long time = end - start;
        times.add(time);
        logger.info(label + ": totalTime=" + time);
//            long sinceTestStart = end - testStart;
//            if (sinceTestStart < 30000) {
//                warmupCount++;
//            }
//
//            if (!warmupEnded && sinceTestStart > 30000) {
//                logger.info("Warm up ended");
//                warmupEnded = true;
//            }
//
//            if (sinceTestStart > 90000) {
//                break;
//            }
//        }
        logger.info("Test complete");
        System.out.println(times.stream()
                .skip(warmupCount).mapToLong(l -> l).summaryStatistics());
    }

    private static class Generator extends AbstractProcessor {

        private static final Pattern PATTERN = Pattern.compile("\\w+");
        int count = 0;

        @Override
        public boolean process(int ordinal, Object item) {
            count++;
            String text = ((Map.Entry<LongWritable, Text>) item).getValue().toString().toLowerCase();
            Matcher m = PATTERN.matcher(text);
            while (m.find()) {
                emit(new AbstractMap.SimpleImmutableEntry<>(m.group(), 1L));
            }
            return true;
        }

        @Override
        public boolean complete() {
            System.out.println("GENERATOR PROCESSED LINE COUNT= " + count);
            return true;
        }
    }

    private static class Combiner extends AbstractProcessor {
        private Map<String, Long> counts = new HashMap<>();
        private Iterator<Map.Entry<String, Long>> iterator;

        @Override
        public boolean process(int ordinal, Object item) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) item;
            counts.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : v + entry.getValue());
            return true;
        }

        @Override
        public boolean complete() {
            if (iterator == null) {
                iterator = counts.entrySet().iterator();
            }

            while (iterator.hasNext() && !getOutbox().isHighWater()) {
                emit(iterator.next());
            }
            return !iterator.hasNext();
        }
    }


//    public static void main(String[] args) throws Exception {
//        if (args.length == 0) {
//            Config config = new Config();
//
//            JoinConfig join = config.getNetworkConfig().getJoin();
//            join.getMulticastConfig().setEnabled(false);
//            join.getTcpIpConfig().setEnabled(true);
//            join.getTcpIpConfig().addMember("server1");
//            join.getTcpIpConfig().addMember("server2");
//            join.getTcpIpConfig().addMember("server3");
//            join.getTcpIpConfig().addMember("server4");
//            join.getTcpIpConfig().addMember("server5");
//            join.getTcpIpConfig().addMember("server6");
//            join.getTcpIpConfig().addMember("server7");
//            join.getTcpIpConfig().addMember("server8");
//            join.getTcpIpConfig().addMember("server9");
//            Hazelcast.newHazelcastInstance(config);
//            return;
//        }
//
//        ClientConfig clientConfig = new ClientConfig();
//        clientConfig.getNetworkConfig().addAddress("server1");
//        clientConfig.getNetworkConfig().addAddress("server2");
//        clientConfig.getNetworkConfig().addAddress("server3");
//        clientConfig.getNetworkConfig().addAddress("server4");
//        clientConfig.getNetworkConfig().addAddress("server5");
//        clientConfig.getNetworkConfig().addAddress("server6");
//        clientConfig.getNetworkConfig().addAddress("server7");
//        clientConfig.getNetworkConfig().addAddress("server8");
//        clientConfig.getNetworkConfig().addAddress("server9");
//
//        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
//
//
//        System.out.println("Press any key to start");
//        System.in.read();
//
//        int taskCount = Runtime.getRuntime().availableProcessors();
//        Vertex generator = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class, taskCount);
//        Vertex counter = createVertex("wordCounter", WordCombinerProcessor.Factory.class, taskCount);
//        Vertex combiner = createVertex("wordCombiner", WordCombinerProcessor.Factory.class, taskCount);
//
////        generator.addSourceTap(new HdfsSourceTap("hdfs", args[0]));
////        String outputPath = args[1] + "_" + System.currentTimeMillis();
////        combiner.addSinkTap(new HdfsSinkTap("hdfs", outputPath));
//
//        JetEngine jetEngine = JetEngine.get(hazelcastInstance, "wordCount");
//        System.out.println("Building application");
//        try {
//            DAG dag = new DAG();
//            dag.addVertex(generator);
//            dag.addVertex(counter);
//            dag.addVertex(combiner);
//
//            dag.addEdge(
//                    new EdgeImpl.EdgeBuilder(
//                            "edge",
//                            generator,
//                            counter)
//                            .processingStrategy(ProcessingStrategy.PARTITIONING)
//                            .build()
//            );
//            dag.addEdge(new EdgeImpl.EdgeBuilder("edge",
//                    counter,
//                    combiner)
//                    .processingStrategy(ProcessingStrategy.PARTITIONING)
//                    .shuffling(true)
//                    .build()
//            );
//            Job job = jetEngine.newJob(dag);
//
//            long t = System.currentTimeMillis();
//            System.out.println("Executing app");
//            job.execute().get();
//            System.out.println("Time=" + (System.currentTimeMillis() - t));
//            System.out.println("OutputPath=" + outputPath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            hazelcastInstance.shutdown();
//        }
//    }
//
//    private static Vertex createVertex(String name, Class processorClass) {
//        return createVertex(name, processorClass, Runtime.getRuntime().availableProcessors());
//    }
//
//    private static Vertex createVertex(String name, Class processorClass, int taskCount) {
//        System.out.println("Creating vertex " + name + " with class " + processorClass + " and task count " + taskCount);
//        return new VertexImpl(
//                name,
//                ProcessorDescriptor.
//                        builder(processorClass).
//                        withTaskCount(taskCount).
//                        build()
//        );
//    }
}
