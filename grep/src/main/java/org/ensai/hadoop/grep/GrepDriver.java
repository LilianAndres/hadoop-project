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

        // Pass regex pattern to mapper
        conf.set("grep.pattern", regex);

        conf.setMapperClass(GrepMapper.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // Mapper-only job (otherwise an identity reducer would have been used)
        conf.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        JobClient.runJob(conf);
    }
}
