package com.mapred.core;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenrePartitioner extends Partitioner<GenreKey, DoubleWritable> {

    @Override
    public int getPartition(GenreKey key, DoubleWritable value, int numPartitions) {
        int i = (key.getMainCategory() + key.getGenre()).hashCode() % numPartitions;
        return Math.abs(i);
    }
}
