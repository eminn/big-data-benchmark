package com.hazelcast.benchmark.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.impl.hazelcast.JetEngine;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.config.JetClientConfig;
import com.hazelcast.jet.spi.config.JetConfig;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.processor.ProcessorDescriptor;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;

import java.util.concurrent.Future;

public class JetWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            JetConfig config = new JetConfig();

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

        JetClientConfig clientConfig = new JetClientConfig();
        clientConfig.getNetworkConfig().addAddress("server1");
        clientConfig.getNetworkConfig().addAddress("server2");
        clientConfig.getNetworkConfig().addAddress("server3");
        clientConfig.getNetworkConfig().addAddress("server4");
        clientConfig.getNetworkConfig().addAddress("server5");
        clientConfig.getNetworkConfig().addAddress("server6");
        clientConfig.getNetworkConfig().addAddress("server7");
        clientConfig.getNetworkConfig().addAddress("server8");
        clientConfig.getNetworkConfig().addAddress("server9");
        JetApplicationConfig appConfig = new JetApplicationConfig("wordCount");
        appConfig.setJetSecondsToAwait(100000);
        appConfig.setChunkSize(4000);
        appConfig.setMaxProcessingThreads(
                Runtime.getRuntime().availableProcessors() / 2);
        clientConfig.addJetApplicationConfig(appConfig);

        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);


        System.out.println("Press any key to start");
        System.in.read();

        int taskCount = Runtime.getRuntime().availableProcessors() / 2;
        Vertex generator = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class, taskCount);
        Vertex counter = createVertex("wordCounter", WordCombinerProcessor.Factory.class, taskCount);
        Vertex combiner = createVertex("wordCombiner", WordCombinerProcessor.Factory.class, taskCount);

        generator.addSourceTap(new HdfsSourceTap("hdfs", args[0]));
        combiner.addSinkTap(new HdfsSinkTap("hdfs", args[1] + "_" + System.currentTimeMillis()));

        Application application = JetEngine.getJetApplication(hazelcastInstance, "wordCount", appConfig);
        System.out.println("Building application");
        try {
            DAG dag = new DAGImpl("wordCount");
            dag.addVertex(generator);
            dag.addVertex(counter);
            dag.addVertex(combiner);

            dag.addEdge(
                    new EdgeImpl.EdgeBuilder(
                            "edge",
                            generator,
                            counter)
                            .processingStrategy(ProcessingStrategy.PARTITIONING)
                            .build()
            );
            dag.addEdge(new EdgeImpl.EdgeBuilder("edge",
                    counter,
                    combiner)
                    .processingStrategy(ProcessingStrategy.PARTITIONING)
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
        return createVertex(name, processorClass, Runtime.getRuntime().availableProcessors());
    }

    private static Vertex createVertex(String name, Class processorClass, int taskCount) {
        System.out.println("Creating vertex " + name + " with class " + processorClass + " and task count " + taskCount);
        return new VertexImpl(
                name,
                ProcessorDescriptor.
                        builder(processorClass).
                        withTaskCount(taskCount).
                        build()
        );
    }

    private static Future executeApplication(DAG dag, Application application) throws Exception {
        application.submit(dag);
        return application.execute();
    }
}
