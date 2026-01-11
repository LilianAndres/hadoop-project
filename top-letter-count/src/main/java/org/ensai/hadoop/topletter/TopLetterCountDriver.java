package org.ensai.hadoop.topletter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TopLetterCountDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopLetterDriver <input> <output>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(TopLetterCountDriver.class);
        conf.setJobName("TopLetterCount");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(TopLetterMapper.class);
        conf.setReducerClass(TopLetterReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);

        // Then, run sort -k2 -nr part-00000 | head -n $(echo "$(wc -l < part-00000)*0.02" | bc)
        // https://stackoverflow.com/questions/20583211/top-n-values-by-hadoop-map-reduce-code
    }
}

