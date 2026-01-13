package org.ensai.hadoop.lettercount;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.Iterator;

public class LetterCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get(); // count the number of values of the given key
        }
        output.collect(key, new IntWritable(sum)); // output the key and its count
    }
}