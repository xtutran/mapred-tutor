package com.mapred.core.secondarysort;

import com.mapred.core.GenreKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortGenreCountMapper extends Mapper<GenreKey, DoubleWritable, GenreKey, DoubleWritable> {
    protected DoubleWritable outValue = new DoubleWritable(1D);

    @Override
    protected void map(GenreKey key, DoubleWritable value, org.apache.hadoop.mapreduce.Mapper<GenreKey, DoubleWritable, GenreKey, DoubleWritable>.Context context) throws IOException, InterruptedException {

        //System.out.println(key.toString());

        key.setValue(value.get());
        context.write(key, outValue);
    }
}
