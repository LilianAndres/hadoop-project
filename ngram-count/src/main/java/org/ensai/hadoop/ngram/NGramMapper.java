package org.ensai.hadoop.ngram;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class NGramMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private int maxN = 5;

    @Override
    public void configure(JobConf conf) {
        maxN = conf.getInt("ngram.max", 5); // get the max N-gram value from the configuration
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String line = value.toString().toLowerCase()
                .replaceAll("[^a-zA-Z\\s]", " ")
                .replaceAll("\\s+", " ").trim();

        if (line.isEmpty()) return;

        String[] words = line.split("\\s+");

        for (int i = 0; i < words.length; i++) {
            StringBuilder sb = new StringBuilder();

            for (int n = 2; n <= maxN; n++) {
                if (i + n > words.length) break; // avoid IndexOutOfBounds error
                sb.setLength(0); // reset the builder for each ngram

                for (int j = 0; j < n; j++) {
                    if (j > 0) sb.append(' ');
                    sb.append(words[i + j]); // add the next word to the builder
                }
                output.collect(new Text(sb.toString()), one);
            }
        }
    }
}
