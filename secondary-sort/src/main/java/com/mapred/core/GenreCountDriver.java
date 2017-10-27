package com.mapred.core;

import com.mapred.main.MainDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * HFile generating driver
 */
public class GenreCountDriver extends Configured implements Tool {

    public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        System.out.println("input: " + inputPath);
        System.out.println("output: " + outputPath);

        Path outPath = new Path(outputPath);
        FileSystem fileSystem = FileSystem.get(conf);

        if (fileSystem.exists(outPath)) {
            System.out.println("Delete " + outputPath);
            fileSystem.delete(outPath, true);
        }

        Job job = Job.getInstance(conf, "Calculate score of genres");

        job.setJarByClass(GenreCountDriver.class);

        job.setMapperClass(GenreCountMapper.class);
        job.setMapOutputKeyClass(GenreKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setPartitionerClass(GenrePartitioner.class);
        job.setReducerClass(GenreCountReducer.class);
        job.setCombinerClass(GenreCountReducer.class);

        job.setOutputKeyClass(GenreKey.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length < 2 || conf.getStrings("com.mapred.columns") == null) {
            MainDriver.printUsageArgument(System.out);
            return 0;
        }

        Job job = createJob(conf, args[0], args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @throws Exception exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new GenreCountDriver(), args);
        System.exit(res);
    }
}

