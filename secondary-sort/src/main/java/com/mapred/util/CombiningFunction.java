package com.mapred.util;

import org.apache.hadoop.io.Writable;

public interface CombiningFunction<VALUE extends Writable> {
    public VALUE combine(VALUE value1, VALUE value2);
}
