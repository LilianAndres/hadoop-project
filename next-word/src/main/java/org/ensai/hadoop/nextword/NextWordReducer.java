package org.ensai.hadoop.nextword;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.*;

public class NextWordReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Map<String, Integer> countMap = new HashMap<>();

        // Count next word occurrences
        while (values.hasNext()) {
            String next = values.next().toString();
            countMap.put(next, countMap.getOrDefault(next, 0) + 1);
        }

        // Convert the Map into a List (for the sort)
        List<Map.Entry<String, Integer>> list = new ArrayList<>(countMap.entrySet());

        // Sort in the descending order (based on the next word frequencies)
        list.sort((a, b) -> b.getValue() - a.getValue());

        // Build the output chain based on the pattern "nextWord:count"
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> entry : list) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(entry.getKey()).append(":").append(entry.getValue());
        }

        output.collect(key, new Text(sb.toString()));
    }
}

