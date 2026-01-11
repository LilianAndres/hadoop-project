package org.ensai.hadoop.nextword;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class NextWordDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: NextWordDriver <input> <output> <word>");
            System.exit(-1);
        }

        String input = args[0];
        String output = args[1];
        String targetWord = args[2];

        JobConf conf = new JobConf(NextWordDriver.class);
        conf.setJobName("NextWordDriver");

        FileInputFormat.addInputPath(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        conf.set("nextword.target", targetWord);

        conf.setMapperClass(NextWordMapper.class);
        conf.setReducerClass(NextWordReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}

