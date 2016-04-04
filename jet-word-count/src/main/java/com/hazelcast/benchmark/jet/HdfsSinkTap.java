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
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.application.ApplicationListener;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.tap.SinkOutputStream;
import com.hazelcast.jet.spi.dag.tap.SinkTap;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.dag.tap.TapType;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.spi.NodeEngine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.TaskType;

import java.io.IOException;
import java.util.Set;

public class HdfsSinkTap extends SinkTap {

    private final String name;
    private final String path;

    public HdfsSinkTap(String name, String path) {
        this.name = name;
        this.path = path;
    }

    @Override
    public DataWriter[] getWriters(NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
        try {
            int numTasks = containerDescriptor.getVertex().getDescriptor().getTaskCount();
            DataWriter[] writers = new DataWriter[numTasks];

            for (int i = 0; i < numTasks; i++) {
                int taskNumber = getTaskNumber(nodeEngine, i, numTasks);
                System.out.println("Creating writer with task number " + taskNumber);
                JobConf conf = new JobConf();
                JobID jobId = new JobID();

                conf.setOutputFormat(TextOutputFormat.class);
                conf.setOutputCommitter(FileOutputCommitter.class);
                TextOutputFormat.setOutputPath(conf, new Path(path));

                if (taskNumber == 0) {
                    JobContextImpl jobContext = new JobContextImpl(conf, jobId);
                    conf.getOutputCommitter().setupJob(jobContext);
                    registerCommiter(containerDescriptor, jobContext, conf);
                }

                HdfsReporter reporter = new HdfsReporter();
                TaskAttemptID taskAttemptID = new TaskAttemptID("jet", jobId.getId(), TaskType.JOB_SETUP, taskNumber, 0);
                conf.set("mapred.task.id", taskAttemptID.toString());
                conf.setInt("mapred.task.partition", taskNumber);

                TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(conf, taskAttemptID);
                RecordWriter recordWriter = conf.getOutputFormat().getRecordWriter(null, conf, Integer.toString(taskNumber),
                        reporter);
                writers[i] = new HdfsDataWriter(recordWriter, taskAttemptContext, conf.getOutputCommitter(),
                        containerDescriptor.getConfig().getChunkSize(), reporter);
            }
            return writers;
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    private int getTaskNumber(NodeEngine nodeEngine, int index, int numTasks) {
        ClusterService clusterService = nodeEngine.getClusterService();
        Set<Member> members = clusterService.getMembers();
        int memberIndex = MemberUtil.indexOfMember(members, clusterService.getLocalMember());
        int taskOffset = memberIndex * numTasks;
        return taskOffset + index;
    }

    private void registerCommiter(ContainerDescriptor containerDescriptor, final JobContext jobContext, final JobConf conf) {
        containerDescriptor.registerApplicationListener(new ApplicationListener() {
            @Override
            public void onApplicationExecuted(ApplicationContext applicationContext) {

                try {
                    conf.getOutputCommitter().commitJob(jobContext);
                } catch (IOException e) {
                    throw JetUtil.reThrow(e);
                }
            }
        });
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

    @Override
    public boolean isPartitioned() {
        return false;
    }
}
