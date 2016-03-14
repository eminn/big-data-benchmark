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

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.dag.tap.SourceTap;
import com.hazelcast.jet.spi.dag.tap.TapType;
import com.hazelcast.jet.spi.data.DataReader;
import com.hazelcast.jet.spi.data.tuple.TupleFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class HdfsSourceTap extends SourceTap {


    private String name;
    private String path;

    public HdfsSourceTap(String name, String path) {
        this.name = name;
        this.path = path;
    }

    @Override
    public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, TupleFactory tupleFactory) {

        JobConf configuration = new JobConf();
        configuration.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(configuration, new Path(path));

        ClusterService clusterService = containerDescriptor.getNodeEngine().getClusterService();
        Set<Member> members = clusterService.getMembers();

        int taskCount = members.size() * vertex.getDescriptor().getTaskCount();

        try {
            InputSplit[] splits = configuration.getInputFormat().getSplits(configuration, taskCount);

            int memberIndex = MemberUtil.indexOfMember(members, clusterService.getLocalMember());
            List<DataReader> readers = new ArrayList<>(splits.length / members.size() + 1);
            for (int i = memberIndex; i < splits.length; i += members.size()) {
                InputSplit split = splits[i];
                RecordReader recordReader = configuration.getInputFormat()
                        .getRecordReader(split, configuration, new HdfsReporter());
                System.out.println("Using split with index " + i + " " + split);
                HdfsDataReader reader = new HdfsDataReader(name, recordReader, vertex, containerDescriptor.getConfig().getChunkSize());
                readers.add(reader);
            }
            return readers.toArray(new DataReader[readers.size()]);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
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
