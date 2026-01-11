package org.ensai.hadoop.wordlength;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.Iterator;

public class WordLengthReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get(); // count the number of values of the given key
        }
        output.collect(key, new IntWritable(sum)); // output the key and its count
    }
}

