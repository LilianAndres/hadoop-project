package org.ensai.hadoop.wordlength;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class WordLengthMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final IntWritable wordLength = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

        String line = value.toString().toLowerCase()
                .replaceAll("[^a-zA-Z\\s]", " ")
                .replaceAll("\\s+", " ").trim();

        if (line.isEmpty()) return;

        String[] words = line.split("\\s+");

        for (int i = 0; i < words.length - 1; i++) {
            wordLength.set(words[i].length());
            output.collect(wordLength, one);
        }
    }
}
