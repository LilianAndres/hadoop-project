package org.ensai.hadoop.sentencefilter;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class SentenceFilterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();
    private String targetWord;

    @Override
    public void configure(JobConf conf) {
        targetWord = conf.get("sentencefilter.target").toLowerCase();
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String line = value.toString().trim();

        if (line.isEmpty()) return;

        String[] sentences = line.split("(?<=[.!?])\\s+");

        for (String sentence : sentences) {
            String sentenceLower = sentence.toLowerCase();
            if (sentenceLower.contains(targetWord)) {
                outKey.set(targetWord);
                outValue.set(sentence.trim());
                output.collect(outKey, outValue);
            }
        }
    }
}
