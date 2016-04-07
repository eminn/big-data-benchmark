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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCombinerProcessor implements ContainerProcessor<Tuple<Integer, Integer>, Tuple<Integer, Integer>> {


    private static final AtomicInteger taskCounter = new AtomicInteger(0);

    private int lastIndex;

    private int[] cache = new int[1024 * 1024];

    public WordCombinerProcessor() {

    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        taskCounter.set(processorContext.getVertex().getDescriptor().getTaskCount());
    }

    @Override
    public boolean process(ProducerInputStream<Tuple<Integer, Integer>> inputStream,
                           ConsumerOutputStream<Tuple<Integer, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Tuple<Integer, Integer> word : inputStream) {
            this.cache[word.getKey(0)] += word.getValue(0);
        }
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Integer, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        boolean finalized = false;

        try {
            int idx = 0;
            int chunkSize = processorContext.getConfig().getChunkSize();

            while (this.lastIndex < this.cache.length) {
                int count = this.cache[this.lastIndex];

                if (count == 0) {
                    this.lastIndex++;
                    continue;
                }

                outputStream.consume(new Tuple2<>(this.lastIndex++, count));

                if (idx++ == chunkSize - 1) {
                    break;

                }
            }

            finalized = this.lastIndex == this.cache.length;
        } finally {
            if (finalized) {
                this.lastIndex = 0;
                clearCaches();
            }
        }

        return finalized;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    private void clearCaches() {
        Arrays.fill(this.cache, 0);
    }

    public static class Factory implements ContainerProcessorFactory {
        @Override
        public ContainerProcessor getProcessor(Vertex vertex) {
            return new WordCombinerProcessor();
        }
    }
}