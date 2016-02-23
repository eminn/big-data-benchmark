package com.hazelcast.benchmark.generator;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class TestGenerateFile {

    private static final int DISTINCT_WORDS = 100_000;

    @Before
    public void setup() {
        BasicConfigurator.configure();
    }

    @Test @Ignore
    public void generate_10m_File() throws IOException {
        DataOutputStream hdfsFile = Hdfs.getHdfsFile("10m_words.txt");

        WordGenerator.writeToFile(new OutputStreamWriter(hdfsFile), DISTINCT_WORDS, 10_000_000);
        hdfsFile.flush();
        hdfsFile.close();
    }

    @Test @Ignore
    public void generate_100m_File() throws IOException {
        DataOutputStream hdfsFile = Hdfs.getHdfsFile("100m_words.txt");

        WordGenerator.writeToFile(new OutputStreamWriter(hdfsFile), DISTINCT_WORDS, 100_000_000);
        hdfsFile.flush();
        hdfsFile.close();
    }
}
