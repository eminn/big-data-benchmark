package com.hazelcast.benchmark.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by can on 22/02/2016.
 */
public class Hdfs {
    public static DataOutputStream getHdfsFile(String name) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        Path path = new Path(name);

        return fs.create(path);
    }
}
