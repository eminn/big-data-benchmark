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

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.Iterator;

public class HdfsDataWriter implements DataWriter {

    private final RecordWriter recordWriter;
    private final TaskAttemptContextImpl taskAttemptContext;
    private final OutputCommitter outputCommitter;
    private int chunkSize;
    private final Reporter reporter;
    private final DefaultObjectIOStream<Object> chunkInputStream;
    private int lastConsumedCount;
    private boolean closed;
    private boolean isFlushed;

    public HdfsDataWriter(RecordWriter writer, TaskAttemptContextImpl taskAttemptContext, OutputCommitter outputCommitter, int chunkSize, Reporter reporter) {
        this.recordWriter = writer;
        this.taskAttemptContext = taskAttemptContext;
        this.outputCommitter = outputCommitter;
        this.chunkSize = chunkSize;
        this.reporter = reporter;
        this.chunkInputStream = new DefaultObjectIOStream<>(new Tuple[chunkSize]);
    }

    @Override
    public SinkTapWriteStrategy getSinkTapWriteStrategy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public int getPartitionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> producerInputStream) throws Exception {
        chunkInputStream.consumeStream(producerInputStream);
        lastConsumedCount = chunkInputStream.size();
        this.isFlushed = false;
        return lastConsumedCount;
    }

    @Override
    public int consumeObject(Object o) throws Exception {
        chunkInputStream.consume(o);
        lastConsumedCount = 1;
        this.isFlushed = false;
        return lastConsumedCount;
    }

    @Override
    public boolean isShuffled() {
        return false;
    }

    @Override
    public int flush() {
        for (Iterator<Object> iterator = chunkInputStream.iterator(); iterator.hasNext(); ) {
            Tuple tuple = (Tuple) iterator.next();
            try {
                recordWriter.write(tuple.getKey(0), tuple.getValue(0));
            } catch (IOException e) {
                throw JetUtil.reThrow(e);
            }
        }
        int size = chunkInputStream.size();
        chunkInputStream.reset();
        isFlushed = true;

        if (size < chunkSize) {
            close();
        }
        return size;
    }

    @Override
    public boolean isFlushed() {
        if (!isFlushed) flush();
        return isFlushed;
    }

    @Override
    public void open() {
        closed = false;
        isFlushed = true;
    }

    @Override
    public void close() {
        closed = true;
        try {
            recordWriter.close(reporter);

            if (outputCommitter.needsTaskCommit(taskAttemptContext)) {
                outputCommitter.commitTask(taskAttemptContext);
            }

        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public int lastConsumedCount() {
        return lastConsumedCount;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return null;
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return null;
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return null;
    }

    @Override
    public boolean consume(ProducerInputStream<Object> objects) throws Exception {
        return consumeChunk(objects) > 0;
    }
}
