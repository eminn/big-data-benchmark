package com.hazelcast.benchmark.jet;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.impl.hazelcast.JetEngine;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.config.JetConfig;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.processor.ProcessorDescriptor;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.concurrent.Future;

public class JetWordCount {

    public static void main(String[] args) throws Exception {
        JetApplicationConfig appConfig = new JetApplicationConfig("wordCount");
        appConfig.setJetSecondsToAwait(100000);
        appConfig.setChunkSize(4000);
        appConfig.setMaxProcessingThreads(
                Runtime.getRuntime().availableProcessors());

        JetConfig config = new JetConfig();
        config.addJetApplicationConfig(appConfig);

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
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        if (args.length == 0) {
            return;
        }

        System.out.println("Press any key to start");
        System.in.read();

        Vertex generator = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class);
        Vertex counter = createVertex("wordCounter", WordCombinerProcessor.Factory.class);
        Vertex combiner = createVertex("wordCombiner", WordCombinerProcessor.Factory.class);

        JobConf conf = new JobConf();

        // TODO
        int taskNumber = 1;
        TaskAttemptID taskAttemptId = TaskAttemptID.forName("attempt__0000_r_"
                + String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s", " ").replace(" ", "0")
                + Integer.toString(taskNumber + 1)
                + "_0");

        TextOutputFormat.setOutputPath(conf, new Path(args[1]));
        generator.addSourceTap(new HdfsSourceTap("hdfs", args[0]));
        combiner.addSinkFile("output.txt");
//        counter.addSinkTap(new HdfsSinkTap("hdfs", conf));

        Application application = JetEngine.getJetApplication(hazelcastInstance, "wordCount");
        try {
            DAG dag = new DAGImpl("wordCount");
            dag.addVertex(generator);
            dag.addVertex(counter);
            dag.addVertex(combiner);
            HashingStrategy<Tuple<String,Integer>, String> hashingStrategy =
                    new HashingStrategy<Tuple<String, Integer>, String>() {
                        @Override
                        public int hash(Tuple<String, Integer> object, String partitionKey, ContainerDescriptor containerDescriptor) {
                            return partitionKey.hashCode();
                        }
                    };
            PartitioningStrategy<Tuple<String, Integer>> partitioningStrategy =
                    new PartitioningStrategy<Tuple<String, Integer>>() {
                        @Override
                        public Object getPartitionKey(Tuple<String,Integer> key) {
                            return key.getKey(0);
                        }
                    };
            dag.addEdge(
                    new EdgeImpl.EdgeBuilder(
                            "edge",
                            generator,
                            counter)
                            .processingStrategy(ProcessingStrategy.PARTITIONING)
                            .hashingStrategy(hashingStrategy)
                            .partitioningStrategy(partitioningStrategy)
                    .build()
            );
            dag.addEdge(new EdgeImpl.EdgeBuilder("edge",
                    counter,
                    combiner)
                    .processingStrategy(ProcessingStrategy.PARTITIONING)
                    .hashingStrategy(hashingStrategy)
                    .partitioningStrategy(partitioningStrategy)
                    .shuffling(true)
                    .build()
            );
            long t = System.currentTimeMillis();
            System.out.println("Executing app");
            executeApplication(dag, application).get();
            System.out.println("Time=" + (System.currentTimeMillis() - t));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            application.finalizeApplication().get();
            hazelcastInstance.shutdown();
        }
    }

    private static Vertex createVertex(String name, Class processorClass) {
        return new VertexImpl(
                name,
                ProcessorDescriptor.
                        builder(processorClass).
                        withTaskCount(Runtime.getRuntime().availableProcessors()).
                        build()
        );
    }

    private static Future executeApplication(DAG dag, Application application) throws Exception {
        application.submit(dag);
        return application.execute();
    }
}
