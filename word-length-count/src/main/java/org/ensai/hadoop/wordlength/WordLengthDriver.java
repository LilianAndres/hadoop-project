package org.ensai.hadoop.wordlength;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

public class WordLengthDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WordLengthDriver <input path> <output path>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(WordLengthDriver.class);
        conf.setJobName("WordLengthCount");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(WordLengthMapper.class);
        conf.setReducerClass(WordLengthReducer.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}
