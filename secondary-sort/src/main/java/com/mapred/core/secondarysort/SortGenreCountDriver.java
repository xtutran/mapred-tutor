package com.mapred.core.secondarysort;

import com.mapred.core.GenreKey;
import com.mapred.main.MainDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author tutx1
 */
public class SortGenreCountDriver extends Configured implements Tool {

    public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        System.out.println("input: " + inputPath);
        System.out.println("output: " + outputPath);

        Path outPath = new Path(outputPath);
        FileSystem fileSystem = FileSystem.newInstance(new Configuration(true));

        if (fileSystem.exists(outPath)) {
            System.out.println("Delete " + outputPath);
            fileSystem.delete(outPath, true);
        }

        Job job = new Job(conf, "Sorting top genre by value");
        job.setJarByClass(SortGenreCountDriver.class);

        job.setMapperClass(SortGenreCountMapper.class);
        job.setMapOutputKeyClass(GenreKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setPartitionerClass(SortGenreCountPartioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setReducerClass(SortGenreCountReducer.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
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

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SortGenreCountDriver(), args);
        System.exit(res);
    }
}

