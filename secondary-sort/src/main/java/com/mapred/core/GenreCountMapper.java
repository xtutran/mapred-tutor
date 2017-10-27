package com.mapred.core;

import com.mapred.parser.InputParser;
import com.mapred.parser.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author TuTX1
 */
public class GenreCountMapper extends Mapper<LongWritable, Text, GenreKey, DoubleWritable> {
    protected DoubleWritable outValue = new DoubleWritable(1D);
    protected GenreKey outKey = new GenreKey();
    protected InputParser parser = InputParser.getInstance();

    public static final Logger LOG = LoggerFactory.getLogger(GenreCountMapper.class);

    @Override
    protected void setup(Mapper<LongWritable, Text, GenreKey, DoubleWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        if (conf == null) {
            LOG.error("Missing configuration");
            return;
        }

        parser.loadHadoopConf(conf);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, GenreKey, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String interpretedEventType;
        String mainCategory;
        String genre1;
        String genre2;
        String genre3;
        int viewedDuration;
        int runLength;

        parser.parse(value.toString());

        String eventTimeStr = parser.getValue("event_time");
        if (eventTimeStr == null || !parser.existOnTimeRange(eventTimeStr)) {
            context.getCounter(Counter.NULL_EVENTTIME).increment(1);
            return;
        }

        interpretedEventType = parser.getValue("event_type");
        if (interpretedEventType == null) {
            context.getCounter(Counter.NULL_EVENTTYPE).increment(1);
            return;
        }

        mainCategory = parser.getValue("main_category");
        if (mainCategory == null || mainCategory.equals("Adult")) {
            context.getCounter(Counter.FILTERED_CATEGORY).increment(1);
            return;
        }

        genre1 = parser.getValue("genre1");
        if (genre1 == null) {
            context.getCounter(Counter.NULL_GENRE1).increment(1);
            return;
        }

        genre2 = parser.getValue("genre2");
        if (genre2 == null) {
            context.getCounter(Counter.NULL_GENRE2).increment(1);
            return;
        }

        genre3 = parser.getValue("genre3");
        if (genre3 == null) {
            context.getCounter(Counter.NULL_GENRE3).increment(1);
            return;
        }

        String duration = parser.getValue("duration");
        if (duration == null) {
            context.getCounter(Counter.NULL_DURATION).increment(1);
            return;
        }
        viewedDuration = StringUtils.parseInt(duration, "\\N");

        String runlength = parser.getValue("runlength");
        System.out.println(runlength);
        if (runlength == null) {
            context.getCounter(Counter.NULL_RUNLENGTH).increment(1);
            return;
        }
        runLength = StringUtils.parseInt(runlength, "\\N");

        // logic of map
        if (runLength < 0) {
            return;
        } else if (runLength == 0 && viewedDuration > 0) {
            double weight = (double) viewedDuration / 60;
            outValue.set(weight);
        } else if ("Watch".equals(interpretedEventType) && runLength > 0) {
            double weight = (double) viewedDuration / (double) runLength;
            outValue.set(weight);
        } else {
            outValue.set(1D);
        }

        if (!genre1.isEmpty()) {
            outKey.set(mainCategory, genre1);
            context.write(outKey, outValue);
        }

        if (!genre2.isEmpty()) {
            outKey.set(mainCategory, genre2);
            context.write(outKey, outValue);
        }

        if (!genre3.isEmpty()) {
            outKey.set(mainCategory, genre3);
            context.write(outKey, outValue);
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, GenreKey, DoubleWritable>.Context context) throws IOException, InterruptedException {
        parser.cleanup();
    }
}
