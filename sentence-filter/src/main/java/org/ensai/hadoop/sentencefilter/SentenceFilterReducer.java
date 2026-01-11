package org.ensai.hadoop.sentencefilter;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.Iterator;

public class SentenceFilterReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            Text sentence = values.next();
            output.collect(key, sentence); // do nothing, emit the same pairs as the Mapper
        }
    }
}

