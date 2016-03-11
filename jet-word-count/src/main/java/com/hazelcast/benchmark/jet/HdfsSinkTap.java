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

import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.tap.SinkOutputStream;
import com.hazelcast.jet.spi.dag.tap.SinkTap;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.dag.tap.TapType;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.spi.NodeEngine;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;

import java.io.IOException;

public class HdfsSinkTap extends SinkTap {

    private final String name;
    private final JobConf conf;

    public HdfsSinkTap(String name, JobConf conf) {
        this.name = name;
        this.conf = conf;
    }

    @Override
    public DataWriter[] getWriters(NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
        try {
            HdfsReporter reporter = new HdfsReporter();


            RecordWriter recordWriter = conf.getOutputFormat().getRecordWriter(null, conf, name, reporter);
            return new DataWriter[]
                    {new HdfsDataWriter(recordWriter, containerDescriptor.getConfig().getChunkSize(), reporter)};
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public SinkTapWriteStrategy getTapStrategy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SinkOutputStream getSinkOutputStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TapType getType() {
        return TapType.OTHER;
    }
}
