package org.ensai.hadoop.grep;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class GrepDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: GrepDriver <input> <output> <pattern>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String regex = args[2];

        JobConf conf = new JobConf(GrepDriver.class);
        conf.setJobName("Grep");

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        // Pass regex pattern to mapper
        conf.set("grep.pattern", regex);

        conf.setMapperClass(GrepMapper.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // Mapper-only job (otherwise an identity reducer would have been used)
        conf.setNumReduceTasks(0);

        JobClient.runJob(conf);
    }
}
