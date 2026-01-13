package org.ensai.hadoop.topk;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TopKDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: TopKDriver <input> <output> <topK>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int topK = Integer.parseInt(args[2]);

        JobConf conf = new JobConf(TopKDriver.class);
        conf.setJobName("TopK");

        conf.setInt("topk.count", topK);

        conf.setMapperClass(TopKMapper.class);
        conf.setReducerClass(TopKReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(1); // make sure everything goes to the same unique reducer

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        JobClient.runJob(conf);
    }
}
