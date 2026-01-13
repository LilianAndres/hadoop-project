package org.ensai.hadoop.lettercount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class LetterCountDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopLetterDriver <input> <output>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(LetterCountDriver.class);
        conf.setJobName("TopLetterCount");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(LetterCountMapper.class);
        conf.setReducerClass(LetterCountReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);

        // Then, run sort something like -k2 -nr part-00000 | head -n $(echo "$(wc -l < part-00000)*0.02" | bc)
        // Please see https://stackoverflow.com/questions/20583211/top-n-values-by-hadoop-map-reduce-code
        // One might also consider the Job chaining approach (not really necessary here).
    }
}

