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
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class HdfsSourceTap extends SourceTap {

    public static final Comparator<Member> MEMBER_COMPARATOR = (Comparator<Member>) (left, right) -> left.getUuid().compareTo(right.getUuid());
    private JobConf configuration;
    private String name;

    public HdfsSourceTap(String name, JobConf configuration) {
        this.configuration = configuration;
        this.name = name;

    }

    @Override
    public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, TupleFactory tupleFactory) {
        int taskCount = vertex.getDescriptor().getTaskCount();

        try {
            InputSplit[] splits = configuration.getInputFormat().getSplits(configuration, taskCount);
            ClusterService clusterService = containerDescriptor.getNodeEngine().getClusterService();
            Member[] members = sortMembers(clusterService.getMembers());
            int memberIndex = Arrays.binarySearch(members, clusterService.getLocalMember(), MEMBER_COMPARATOR);

            List<DataReader> readers = new ArrayList<>(splits.length / members.length + 1);

            for (int i = memberIndex; i < splits.length; i += members.length) {
                InputSplit split = splits[i];
                RecordReader recordReader = configuration.getInputFormat()
                        .getRecordReader(split, configuration, new HdfsReporter());
                HdfsDataReader reader = new HdfsDataReader(name, recordReader, vertex, containerDescriptor.getConfig().getChunkSize());
                readers.add(reader);
            }
            return readers.toArray(new DataReader[readers.size()]);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    private Member[] sortMembers(Set<Member> members) {
        TreeSet<Member> sorted = new TreeSet<>(MEMBER_COMPARATOR);
        sorted.addAll(members);
        return sorted.toArray(new Member[sorted.size()]);
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
