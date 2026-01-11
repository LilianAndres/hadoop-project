package org.ensai.hadoop.nextword;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class NextWordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final Text nextWordKey = new Text();
    private String targetWord;

    @Override
    public void configure(JobConf conf) {
        targetWord = conf.get("nextword.target").toLowerCase();
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String line = value.toString().toLowerCase()
                .replaceAll("[^a-zA-Z\\s]", " ")
                .replaceAll("\\s+", " ").trim();

        if (line.isEmpty()) return;

        String[] words = line.split("\\s+");

        for (int i = 0; i < words.length - 1; i++) {
            if (words[i].equals(targetWord)) {
                nextWordKey.set(words[i + 1]);
                output.collect(nextWordKey, one);
            }
        }
    }
}
