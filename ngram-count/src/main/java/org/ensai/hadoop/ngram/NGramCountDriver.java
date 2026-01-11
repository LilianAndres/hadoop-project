package org.ensai.hadoop.ngram;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class NGramCountDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: NGramCountDriver <input> <output> [maxN]");
            System.exit(-1);
        }

        int maxN = 5; // set a default value

        if (args.length >= 3) {
            maxN = Integer.parseInt(args[2]);
        }

        JobConf conf = new JobConf(NGramCountDriver.class);
        conf.setJobName("NGramCount");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setInt("ngram.max", maxN); // set the maximum N-gram value in the job configuration

        conf.setMapperClass(NGramMapper.class);
        conf.setReducerClass(NGramReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}
