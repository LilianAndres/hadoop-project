package org.ensai.hadoop.topk;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class TopKMapper extends MapReduceBase implements Mapper<Text, IntWritable, Text, Text> {

    // Top-k application is a global application that needs a global view
    // We use a dummyKey so that every pair is sent to a unique and single Reducer
    private static final Text dummyKey = new Text("TopK");
    private final Text entityCount = new Text();

    @Override
    public void map(Text key, IntWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        entityCount.set(key.toString() + "=" + value.get());
        output.collect(dummyKey, entityCount);
    }
}
