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
import com.hazelcast.nio.Address;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
        Member[] members = MemberUtil.sortMembers(clusterService.getMembers());
        int minimumSplitCount = members.length * vertex.getDescriptor().getTaskCount();

        try {
            InputSplit[] splits = configuration.getInputFormat().getSplits(configuration, minimumSplitCount);
            IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.length];
            for (int i = 0; i < splits.length; i++) {
                indexedInputSplits[i] = new IndexedInputSplit(i, splits[i]);
            }

            Map<Address, Collection<IndexedInputSplit>> assigned = assignSplits(indexedInputSplits, members);
            printAssignments(assigned);

            Collection<IndexedInputSplit> assignedSplits = assigned.get(clusterService.getThisAddress());
            if (assignedSplits == null) {
                return new DataReader[0];
            }

            List<DataReader> readers = new ArrayList<>(assignedSplits.size());
            for (IndexedInputSplit split : assignedSplits) {
                RecordReader recordReader = configuration.getInputFormat()
                        .getRecordReader(split.getSplit(), configuration, new HdfsReporter());
                HdfsDataReader reader = new HdfsDataReader(name, recordReader,
                        vertex, containerDescriptor.getConfig().getChunkSize());
                readers.add(reader);
            }
            return readers.toArray(new DataReader[readers.size()]);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    private void printAssignments(Map<Address, Collection<IndexedInputSplit>> assigned) throws IOException {
        for (Map.Entry<Address, Collection<IndexedInputSplit>> entry : assigned.entrySet()) {
            StringBuilder builder = new StringBuilder();
            builder.append(entry.getKey()).append(":\n");

            Collection<IndexedInputSplit> list = entry.getValue();
            if (list != null) {
                for (IndexedInputSplit inputSplit : list) {
                    builder.append(inputSplit);
                    builder.append("\n");
                }
            }
            System.out.println(builder);
        }
    }

    private boolean isSplitLocalForMember(Member member, InputSplit split) throws IOException {
        String[] locations = split.getLocations();
        for (String location : locations) {
            for (InetAddress inetAddress : InetAddress.getAllByName(location)) {
                if (member.getAddress().getInetAddress().equals(inetAddress)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Map<Address, Collection<IndexedInputSplit>> assignSplits(IndexedInputSplit[] inputSplits, Member[] sortedMembers) throws IOException {
        Map<IndexedInputSplit, List<Integer>> assignments = new TreeMap<>();
        int[] counts = new int[sortedMembers.length];

        // assign local members
        for (IndexedInputSplit inputSplit : inputSplits) {
            List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < sortedMembers.length; i++) {
                if (isSplitLocalForMember(sortedMembers[i], inputSplit.getSplit())) {
                    indexes.add(i);
                    counts[i]++;
                }
            }
            assignments.put(inputSplit, indexes);
        }
        System.out.println("Sorted members: " + Arrays.toString(sortedMembers));

        // assign all remaining splits to member with lowest number of splits
        for (Map.Entry<IndexedInputSplit, List<Integer>> entry : assignments.entrySet()) {
            List<Integer> indexes = entry.getValue();
            if (indexes.isEmpty()) {
                int indexToAdd = findMinIndex(counts);
                indexes.add(indexToAdd);
                counts[indexToAdd]++;
            }
        }
        System.out.println("Counts before pruning: " + Arrays.toString(counts));

        // prune addresses for splits with more than one member assigned
        boolean found;
        do {
            found = false;
            for (Map.Entry<IndexedInputSplit, List<Integer>> entry : assignments.entrySet()) {
                List<Integer> indexes = entry.getValue();
                if (indexes.size() > 1) {
                    found = true;
                    // find member with most number of splits and remove from list
                    int maxIndex = findMaxIndex(counts, indexes);
                    indexes.remove((Object)maxIndex);
                    counts[maxIndex]--;
                }
            }
        } while (found);

        System.out.println("Final counts=" + Arrays.toString(counts));
        // assign to map
        Map<Address, Collection<IndexedInputSplit>> mapToSplit = new HashMap<>();
        for (Map.Entry<IndexedInputSplit, List<Integer>> entry : assignments.entrySet()) {
            IndexedInputSplit split = entry.getKey();
            List<Integer> indexes = entry.getValue();

            if (indexes.size() != 1) {
                throw new RuntimeException("Split " + split + " has " + indexes.size() + " assignments");
            }

            // add input split to final assignment list
            Integer memberIndex = indexes.get(0);
            Address address = sortedMembers[memberIndex].getAddress();
            Collection<IndexedInputSplit> assigned = mapToSplit.get(address);
            if (assigned != null) {
                assigned.add(split);
            } else {
                mapToSplit.put(address, new TreeSet<>(Collections.singletonList(split)));
            }
        }

        return mapToSplit;
    }

    private int findMinIndex(int[] counts) {
        int index = 0;
        for (int i = 1; i < counts.length; i++) {
            if (counts[i] < counts[index]) {
                index = i;
            }
        }
        return index;
    }

    private int findMaxIndex(int[] counts, List<Integer> indexes) {
        int maxIndex = indexes.get(0);
        for (int i = 1; i < indexes.size(); i++) {
            Integer comparedIndex = indexes.get(i);
            if (counts[comparedIndex] > counts[maxIndex]) {
                maxIndex = comparedIndex;
            }
        }
        return maxIndex;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TapType getType() {
        return TapType.OTHER;
    }

    public static class IndexedInputSplit implements Comparable<IndexedInputSplit> {

        private final int index;
        private final InputSplit split;

        public IndexedInputSplit(int index, InputSplit split) {
            this.index = index;
            this.split = split;
        }

        public InputSplit getSplit() {
            return split;
        }

        public int getIndex() {
            return index;
        }

        @Override
        public String toString() {
            return "IndexedInputSplit{" +
                    "index=" + index +
                    ", split=" + split +
                    '}';
        }

        @Override
        public int compareTo(IndexedInputSplit o) {
            return Integer.compare(index, o.index);
        }
    }

}
