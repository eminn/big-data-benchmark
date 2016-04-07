
/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.benchmark.jet;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.impl.data.tuple.Tuple2;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Iterator;
import java.util.StringTokenizer;

public class WordGeneratorProcessor implements ContainerProcessor<Tuple<LongWritable, Text>, Tuple<String, Integer>> {
    private int idx;
    private Iterator<Tuple<LongWritable, Text>> iterator;
    private StringTokenizer stringTokenizer;

    private long totalProcessTime;
    private long totalFinalizeTime;

    private boolean processStringTokenizer(ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                                           ProcessorContext processorContext) throws Exception {
        while (this.stringTokenizer.hasMoreElements()) {
            String word = this.stringTokenizer.nextToken();

            outputStream.consume(new Tuple2<>(word, 1));

            this.idx++;

            if (this.idx == processorContext.getConfig().getChunkSize()) {
                this.idx = 0;
                return false;
            }
        }

        this.idx = 0;
        this.stringTokenizer = null;
        return true;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        this.idx = 0;
        this.iterator = null;
        this.stringTokenizer = null;
    }

    @Override
    public boolean process(ProducerInputStream<Tuple<LongWritable, Text>> inputStream,
                           ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        long start = System.nanoTime();
        if (this.stringTokenizer != null) {
            processStringTokenizer(outputStream, processorContext);
            totalProcessTime += System.nanoTime() - start;
            return false;
        }

        if (this.iterator == null) {
            this.iterator = inputStream.iterator();
        }

        while (iterator.hasNext()) {
            Tuple<LongWritable, Text> next = iterator.next();

            this.stringTokenizer = new StringTokenizer(next.getValue(0).toString());

            if (!processStringTokenizer(outputStream, processorContext)) {
                totalProcessTime += System.nanoTime() - start;
                return false;
            }
        }

        this.iterator = null;
        totalProcessTime += System.nanoTime() - start;
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        long start = System.nanoTime();
        if (this.stringTokenizer != null) {
            processStringTokenizer(outputStream, processorContext);
            totalFinalizeTime += System.nanoTime() - start;
            return false;
        }

        this.stringTokenizer = null;
        totalFinalizeTime += System.nanoTime() - start;

        System.out.println(processorContext.getVertex().getName() + ".TotalProcessTime=" + totalProcessTime /1000000);
        System.out.println(processorContext.getVertex().getName() + ".TotalFinalizeTime=" + totalFinalizeTime /1000000);
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements ContainerProcessorFactory<Tuple<LongWritable, Text>, Tuple<String,Integer>> {
        @Override
        public ContainerProcessor<Tuple<LongWritable, Text>, Tuple<String,Integer>> getProcessor(Vertex vertex) {
            return new WordGeneratorProcessor();
        }
    }
}