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
    }
}

