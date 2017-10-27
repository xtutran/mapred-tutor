package com.mapred.main;

import com.mapred.core.GenreCountDriver;
import com.mapred.core.secondarysort.SortGenreCountDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.PrintStream;

public class MainDriver extends Configured implements Tool {

    public static void printUsageArgument(PrintStream out) {
        out.println("-conf <configuration file>\t\t\tspecify an application configuration file");
        out.println("<input directory>\t\t\tspecify an application input directory");
        out.println("<tmp directory>\t\t\tspecify an application temporary directory - immediate result");
        out.println("<output directory>\t\t\tspecify an application output directory");
    }

    /**
     * @param args enviroment arguments
     * @throws Exception running time exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MainDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();
        if (args.length < 3 || conf.getStrings("com.mapred.columns") == null) {
            MainDriver.printUsageArgument(System.out);
            return 0;
        }

        Job calculateJob = GenreCountDriver.createJob(conf, args[0], args[1]);
        boolean firstJob = calculateJob.waitForCompletion(true);

        if (firstJob) {
            Job sortedJob = SortGenreCountDriver.createJob(conf, args[1], args[2]);
            boolean secondJob = sortedJob.waitForCompletion(true);
            if (secondJob) {

                //delete temporary resource;
                FileSystem.get(conf).deleteOnExit(new Path(args[1]));

                return 0;
            } else {
                return 1;
            }
        } else {
            return 1;
        }
    }

}
