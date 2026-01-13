package org.ensai.hadoop.nextword;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class NextWordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private final Text currentWord = new Text();
    private final Text nextWord = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String line = value.toString().toLowerCase()
                .replaceAll("[^a-zA-Z\\s]", " ")
                .replaceAll("\\s+", " ").trim();

        if (line.isEmpty()) return;

        String[] words = line.split("\\s+");

        for (int i = 0; i < words.length - 1; i++) {
            currentWord.set(words[i]);
            nextWord.set(words[i + 1]);
            output.collect(currentWord, nextWord); // emit the next word for each word
        }
    }
}
