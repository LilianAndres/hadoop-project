package org.ensai.hadoop.topk;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

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

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setInt("topk.count", topK);

        conf.setMapperClass(TopKMapper.class);
        conf.setReducerClass(TopKReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setNumReduceTasks(1); // make sure everything goes to the same unique reducer

        JobClient.runJob(conf);
    }
}
