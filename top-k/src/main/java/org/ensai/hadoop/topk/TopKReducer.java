package org.ensai.hadoop.topk;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class TopKReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private int K;

    @Override
    public void configure(JobConf job) {
        K = job.getInt("topk.count", -1);
        if (K <= 0) {
            throw new RuntimeException("Parameter 'topk.count' must be set and greater than 0");
        }
    }

    @Override
    public void reduce(Text key, java.util.Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        Map<String, Integer> entityCounts = new HashMap<>();

        while (values.hasNext()) {
            String val = values.next().toString();
            String[] parts = val.split("=");
            if (parts.length != 2) continue;

            String entity = parts[0];
            int count = Integer.parseInt(parts[1]);

            entityCounts.put(entity, entityCounts.getOrDefault(entity, 0) + count);
        }

        // Heap min of size K
        PriorityQueue<Map.Entry<String, Integer>> heap = new PriorityQueue<>(Comparator.comparingInt(Map.Entry::getValue));

        for (Map.Entry<String, Integer> entry : entityCounts.entrySet()) {
            heap.offer(entry); // add the entry to the heap min
            if (heap.size() > K) {
                heap.poll(); // remove the lowest entry of the heap min to keep its size K
            }
        }

        // Need an extra sort for the final output
        Map.Entry<String, Integer>[] topK = heap.toArray(new Map.Entry[0]);
        java.util.Arrays.sort(topK, (a, b) -> b.getValue() - a.getValue());

        for (Map.Entry<String, Integer> entry : topK) {
            output.collect(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
        }
    }
}
