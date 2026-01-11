package org.ensai.hadoop.topletter;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class TopLetterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text letterKey = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String line = value.toString().toLowerCase()
                .replaceAll("[^a-zA-Z\\s]", " ")
                .replaceAll("\\s+", " ").trim();

        if (line.isEmpty()) return;

        String[] words = line.split("\\s+");

        for (String word : words) {
            if (word.length() < 5 || word.length() > 9) continue;

            for (char c : word.toCharArray()) {
                letterKey.set(Character.toString(c));
                output.collect(letterKey, one);
            }
        }
    }
}
