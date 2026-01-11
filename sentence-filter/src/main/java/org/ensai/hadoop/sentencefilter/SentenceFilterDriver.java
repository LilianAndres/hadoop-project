package org.ensai.hadoop.sentencefilter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SentenceFilterDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: SentenceFilterDriver <input> <output> <word>");
            System.exit(-1);
        }

        String input = args[0];
        String output = args[1];
        String targetWord = args[2];

        JobConf conf = new JobConf(SentenceFilterDriver.class);
        conf.setJobName("SentenceFilter");

        FileInputFormat.addInputPath(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        conf.set("sentencefilter.target", targetWord); // set the target word in the job configuration

        conf.setMapperClass(SentenceFilterMapper.class);

        // The Reducer does not do anything here.
        // The best option would have been to make this Job as Mapper-only Job
        // conf.setNumReduceTasks(0);

        conf.setReducerClass(SentenceFilterReducer.class); // keep it only for the project purpose

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        JobClient.runJob(conf);
    }
}

