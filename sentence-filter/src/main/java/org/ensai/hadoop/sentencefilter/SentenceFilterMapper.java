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
        targetWord = conf.get("sentencefilter.target").toLowerCase(); // get the target word from the job configuration
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String line = value.toString().trim(); // remove leading and trailing whitespaces

        if (line.isEmpty()) return;

        String[] sentences = line.split("(?<=[.!?])\\s+"); // split the line into sentences

        for (String sentence : sentences) {
            String sentenceLower = sentence.toLowerCase();
            if (sentenceLower.contains(targetWord)) {
                outKey.set(targetWord); // set the key to the target word so every pairs go to the same Reducer (avoid overhead)
                outValue.set(sentence.trim()); // set the value to the sentence that contains the target word
                output.collect(outKey, outValue);
            }
        }
    }
}
