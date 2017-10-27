package com.mapred.core.secondarysort;

import com.mapred.core.GenreKey;
import com.mapred.dto.KeyValue;
import com.mapred.parser.JsonUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortGenreCountReducer extends Reducer<GenreKey, DoubleWritable, Text, NullWritable> {
    protected Text outKey = new Text();

    protected String currentMaincateogry = null;
    protected List<KeyValue> keyValues;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer<GenreKey, DoubleWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        keyValues = new ArrayList<KeyValue>();
    }

    @Override
    protected void reduce(GenreKey key, Iterable<DoubleWritable> values, Reducer<GenreKey, DoubleWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        if (currentMaincateogry == null) {
            currentMaincateogry = key.getMainCategory();
        }
        keyValues.add(new KeyValue(key.getValue(), key.getGenre()));


        //outKey.set(key.toString());
        //context.write(outKey, NullWritable.get());
    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer<GenreKey, DoubleWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        if (currentMaincateogry != null) {
            String key = currentMaincateogry + ";Vector;" + JsonUtil.convertObjectToJson(keyValues);
            outKey.set(key);
            context.write(outKey, NullWritable.get());
        }
    }
} 