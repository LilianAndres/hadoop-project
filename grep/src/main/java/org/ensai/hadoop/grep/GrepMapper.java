package org.ensai.hadoop.grep;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.regex.Pattern;

public class GrepMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private Pattern pattern;

    @Override
    public void configure(JobConf job) {
        String regex = job.get("grep.pattern"); // get the pattern from the job configuration
        if (regex == null || regex.isEmpty()) {
            throw new RuntimeException("Parameter 'grep.pattern' must be provided");
        }
        pattern = Pattern.compile(regex);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String line = value.toString();

        if (pattern.matcher(line).find()) {
            output.collect(value, new Text("")); // emit the lines that match the pattern
        }
    }
}