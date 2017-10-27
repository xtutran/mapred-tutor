package com.mapred.core.secondarysort;

import com.mapred.core.GenreKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortGenreCountPartioner extends Partitioner<GenreKey, DoubleWritable> {

    @Override
    public int getPartition(GenreKey key, DoubleWritable value, int numPartitions) {
        int i = (key.getMainCategory()).hashCode() % numPartitions;
        return Math.abs(i);
    }
}
