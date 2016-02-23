package com.hazelcast.benchmark.jet;

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
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.processor.ProcessorDescriptor;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;

import java.nio.file.Files;
import java.util.concurrent.Future;

public class JetWordCount {

    public static void main(String [] args) throws Exception {
        JetApplicationConfig appConfig = new JetApplicationConfig("wordCount");
        appConfig.setJetSecondsToAwait(100000);
        appConfig.setChunkSize(4000);
        appConfig.setMaxProcessingThreads(
                Runtime.getRuntime().availableProcessors());
        appConfig.setLocalizationDirectory("/tmp/wordcount");

        JetConfig config = new JetConfig();
        config.addJetApplicationConfig(appConfig);

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        Vertex vertex1 = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class);
        Vertex vertex2 = createVertex("wordCounter", WordCounterProcessor.Factory.class);

        vertex1.addSourceFile(args[0]);
        vertex2.addSinkFile(args[1]);

        Application application = JetEngine.getJetApplication(hazelcastInstance, "wordCount");
        try {
            DAG dag = new DAGImpl("wordCount");
            dag.addVertex(vertex1);
            dag.addVertex(vertex2);
            dag.addEdge(
                    new EdgeImpl.EdgeBuilder(
                            "edge",
                            vertex1,
                            vertex2
                    )
                            .processingStrategy(ProcessingStrategy.PARTITIONING)
                            .hashingStrategy(new HashingStrategy<String, String>() {
                                                 @Override
                                                 public int hash(String object,
                                                                 String partitionKey,
                                                                 ContainerDescriptor containerDescriptor) {
                                                     return partitionKey.hashCode();
                                                 }
                                             }
                            )
                            .partitioningStrategy(new PartitioningStrategy<String>() {
                                @Override
                                public String getPartitionKey(String key) {
                                    return key;
                                }
                            })
                            .build()
            );
            long t = System.currentTimeMillis();
            executeApplication(dag, application).get();
            System.out.println("Time=" + (System.currentTimeMillis() - t));
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
