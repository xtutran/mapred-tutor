package com.mapred.core;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

class GenreCountReducer extends Reducer<GenreKey, DoubleWritable, GenreKey, DoubleWritable> {
    protected DoubleWritable outValue = new DoubleWritable();

    @Override
    protected void reduce(GenreKey key, Iterable<DoubleWritable> values, Reducer<GenreKey, DoubleWritable, GenreKey, DoubleWritable>.Context context) throws IOException, InterruptedException {
        Iterator<DoubleWritable> it = values.iterator();

        double sum = 0;

        while (it.hasNext()) {
            sum += it.next().get();
        }

        outValue.set(sum);
        context.write(key, outValue);
    }

}
