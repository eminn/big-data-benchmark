package com.hazelcast.benchmark.generator;

import org.apache.commons.lang.RandomStringUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

/**
 * Created by can on 22/02/2016.
 */
public class WordGenerator {

    private static final Random random = new Random();

    public static void writeToFile(OutputStreamWriter stream, int distinctWords, int numWords) throws IOException {
        String [] words = new String[distinctWords];
        for (int i = 0; i < distinctWords; i++) {
            words[i] = RandomStringUtils.randomAlphabetic(random.nextInt(10) + 2);
        }

        for (int i = 0; i < numWords; i++) {
            stream.write(words[random.nextInt(distinctWords)]);
            if (i % 20 == 0) {
                stream.write("\n");
            } else {
                stream.write(" ");
            }
        }
    }
}
