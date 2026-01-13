package org.ensai.hadoop.nextword;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class NextWordDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: NextWordDriver <input> <output>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        JobConf conf = new JobConf(NextWordDriver.class);
        conf.setJobName("NextWord");

        conf.setMapperClass(NextWordMapper.class);
        conf.setReducerClass(NextWordReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        JobClient.runJob(conf);
    }
}

