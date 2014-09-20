package com.codingserbia;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import com.codingserbia.data.CustomerTestDataProvider;
import com.codingserbia.writable.CustomerSessionWritable;

public class CustomerRecordsReducerTest extends AbstractMapReduceTest {

    private ReduceDriver<LongWritable, CustomerSessionWritable, NullWritable, Text> reduceDriver;

    @Override
    @Before
    public void setUp() {
        super.setUp();

        CustomerRecordsReducer reducer = new CustomerRecordsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    // testing style: tell the input, assert the output
    @Test
    public void testReducerWithManualAssertions() throws Exception {
        reduceDriver.withInput(new LongWritable(4L),
                        CustomerTestDataProvider.CUSTOMER_RECORDS_FOR_REDUCER_INPUT_CATEGORY_4);
        reduceDriver.withInput(new LongWritable(5L),
                        CustomerTestDataProvider.CUSTOMER_RECORDS_FOR_REDUCER_INPUT_CATEGORY_5);
        reduceDriver.withInput(new LongWritable(8L),
                        CustomerTestDataProvider.CUSTOMER_RECORDS_FOR_REDUCER_INPUT_CATEGORY_8);

        Pair<NullWritable, Text> category4Tuple = new Pair<NullWritable, Text>(NullWritable.get(),
                        CustomerTestDataProvider.CATEGORY_4_MAPREDUCE_OUTPUT);
        Pair<NullWritable, Text> category5Tuple = new Pair<NullWritable, Text>(NullWritable.get(),
                        CustomerTestDataProvider.CATEGORY_5_MAPREDUCE_OUTPUT);
        Pair<NullWritable, Text> category8Tuple = new Pair<NullWritable, Text>(NullWritable.get(),
                        CustomerTestDataProvider.CATEGORY_8_MAPREDUCE_OUTPUT);

        List<Pair<NullWritable, Text>> result = reduceDriver.run();

        Assertions.assertThat(result).isNotNull().hasSize(3).contains(category4Tuple, category5Tuple, category8Tuple);
    }
}
