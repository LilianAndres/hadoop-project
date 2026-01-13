package org.ensai.hadoop.lettercount;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class LetterCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

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
            if (word.length() < 5 || word.length() > 9) continue; // keep only the words that are longer than 4 letters and shorter than 10 letters

            for (char c : word.toCharArray()) {
                letterKey.set(Character.toString(c));
                output.collect(letterKey, one); // set the key to the letter and the value to one
            }
        }
    }
}
