package com.hazelcast.benchmark.generator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

/**
 * Created by can on 22/02/2016.
 */
public class WordGenerator {

    public static void writeToFile(OutputStreamWriter stream, int distinctWords, int numWords) throws IOException {
        for (int i = 0; i < numWords; i++) {
            stream.write(i % distinctWords + "");
            if (i % 20 == 0) {
                stream.write("\n");
            } else {
                stream.write(" ");
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: word-generator <path> <num distinct> <num words>");
            return;
        }

        DataOutputStream hdfsFile = Hdfs.getHdfsFile(args[0]);

        WordGenerator.writeToFile(new OutputStreamWriter(hdfsFile), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        hdfsFile.flush();
        hdfsFile.close();
    }
}
