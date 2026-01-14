package org.ensai.hadoop.topk;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class TopKMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    // Top-k application is a global application that needs a global view
    // We use a dummyKey so that every pair is sent to a unique and single Reducer
    private static final Text dummyKey = new Text("TopK");
    private final Text entity = new Text();
    private final IntWritable count = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split("\\s+");
        if (parts.length < 2) return;

        entity.set(parts[0]);
        count.set(Integer.parseInt(parts[1]));

        output.collect(dummyKey, new Text(entity + "=" + count));
    }
}
