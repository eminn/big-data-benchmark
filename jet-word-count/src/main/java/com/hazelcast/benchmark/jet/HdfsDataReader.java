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

import com.hazelcast.jet.api.actor.ProducerCompletionHandler;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.data.tuple.Tuple2;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.DataReader;
import com.hazelcast.jet.spi.strategy.DataTransferringStrategy;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class HdfsDataReader implements DataReader {

    private final String name;
    private final RecordReader recordReader;
    private final Object[] buffer;
    private final Vertex vertex;
    private final int chunkSize;
    private final int id;
    private final List<ProducerCompletionHandler> completionHandlers;
    private boolean closed;
    private int lastProducedCount;

    private long totalTime = 0;


    public HdfsDataReader(String name, RecordReader recordReader, Vertex vertex, int chunkSize, int id) {
        this.name = name;
        this.recordReader = recordReader;
        this.vertex = vertex;
        this.chunkSize = chunkSize;
        this.id = id;
        this.buffer = new Object[chunkSize];

        completionHandlers = new CopyOnWriteArrayList<>();
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean readFromPartitionThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] read() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] produce() {
        int idx = 0;

        long start = System.nanoTime();
        boolean read;
        try {
            do {
                Object key = recordReader.createKey();
                Object value = recordReader.createValue();
                read = recordReader.next(key, value);

                if (read) {
                    buffer[idx++] = new Tuple2<>(key, value);
                }

            } while (read && idx < chunkSize);

            if (idx == 0) {
                lastProducedCount = idx;
                totalTime += System.nanoTime() - start;
                handleProducerCompleted();
                return null;
            } else if (idx == chunkSize) {
                lastProducedCount = idx;
                totalTime += System.nanoTime() - start;
                return buffer;
            } else {
                Object[] truncated = new Object[idx];
                System.arraycopy(buffer, 0, truncated, 0, idx);
                lastProducedCount = idx;
                totalTime += System.nanoTime() - start;
                handleProducerCompleted();
                return truncated;
            }
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public int getPartitionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastProducedCount() {
        return lastProducedCount;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void open() {
        this.closed = false;
    }

    @Override
    public void close() {
        this.closed = true;
        try {
            System.out.println("HdfsDataReader."+ id + "=" + totalTime/1000000);
            this.recordReader.close();
        } catch (IOException e) {
            JetUtil.reThrow(e);
        }
    }


    @Override
    public DataTransferringStrategy getDataTransferringStrategy() {
        return ByReferenceDataTransferringStrategy.INSTANCE;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        completionHandlers.add(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : this.completionHandlers) {
            handler.onComplete(this);
        }
    }
}
