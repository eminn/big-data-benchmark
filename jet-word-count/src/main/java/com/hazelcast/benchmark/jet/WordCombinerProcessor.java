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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCombinerProcessor implements ContainerProcessor<Tuple<String, Integer>, Tuple<String, Integer>> {


    private static final AtomicInteger taskCounter = new AtomicInteger(0);

    private Iterator<String> finalizationIterator;

    private Map<String, Integer> cache = new HashMap<>();

    public WordCombinerProcessor() {

    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        taskCounter.set(processorContext.getVertex().getDescriptor().getTaskCount());
    }

    @Override
    public boolean process(ProducerInputStream<Tuple<String, Integer>> inputStream,
                           ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Tuple<String, Integer> word : inputStream) {
            Integer value = this.cache.get(word.getKey(0));
            if (value == null) {
                this.cache.put(word.getKey(0), word.getValue(0));
            } else {
                this.cache.put(word.getKey(0), value + word.getValue(0));
            }
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        boolean finalized = false;

        try {
            if (finalizationIterator == null) {
                this.finalizationIterator = this.cache.keySet().iterator();
            }

            int idx = 0;
            int chunkSize = processorContext.getConfig().getChunkSize();

            while (this.finalizationIterator.hasNext()) {
                String word = (String) this.finalizationIterator.next();

                outputStream.consume(new Tuple2<>(word, this.cache.get(word)));

                if (idx == chunkSize - 1) {
                    break;
                }

                idx++;
            }

            finalized = !this.finalizationIterator.hasNext();
        } finally {
            if (finalized) {
                this.finalizationIterator = null;
                clearCaches();
            }
        }

        return finalized;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    private void clearCaches() {
        this.cache.clear();
    }

    public static class Factory implements ContainerProcessorFactory {
        @Override
        public ContainerProcessor getProcessor(Vertex vertex) {
            return new WordCombinerProcessor();
        }
    }
}