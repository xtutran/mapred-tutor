package com.mapred.core;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Properties;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author TuTX1
 */
public class FilterMapperTest {
    private GenreCountMapper mapper;
    private Mapper<LongWritable, Text, GenreKey, DoubleWritable>.Context context;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws IOException, InterruptedException {
        mapper = new GenreCountMapper();
        context = mock(Context.class);
        org.apache.hadoop.mapreduce.Counter counter = mock(org.apache.hadoop.mapreduce.Counter.class);
        for (Counter c : Counter.values()) {
            when(context.getCounter(c)).thenReturn(counter);
        }

        Properties property = new Properties();
        property.load(getClass().getResourceAsStream("/config/config.properties"));

        mapper.parser.loadLocalConf(property);
        mapper.outKey = mock(GenreKey.class);
    }

    @Test
    public void testSingleLine() throws IOException, InterruptedException {
        mapper.map(new LongWritable(1), new Text("cust1;chanel1;20170925143600;Play_Back;TV;Action;Fantasy;Fiction;60;"), context);

        InOrder inOrder = inOrder(mapper.outKey, context);
        assertCountedOnce(inOrder, "Action", new DoubleWritable(1));

        inOrder = inOrder(mapper.outKey, context);
        assertCountedOnce(inOrder, "Fantasy", new DoubleWritable(1));
    }

    private void assertCountedOnce(InOrder inOrder, String genre, DoubleWritable weight) throws IOException, InterruptedException {
        inOrder.verify(mapper.outKey).set(eq("TV"), eq(genre));
        inOrder.verify(context).write(eq(mapper.outKey), eq(weight));
    }
}