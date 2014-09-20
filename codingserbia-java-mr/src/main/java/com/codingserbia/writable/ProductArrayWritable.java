package com.codingserbia.writable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class ProductArrayWritable extends ArrayWritable {

    public ProductArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    @Override
    public String toString() {
        String value = "ProductArrayWritable [";
        Writable[] pwArray = get();
        for (Writable pw : pwArray) {
            value += pw.toString();
        }
        value += "]";
        return value;
    }

}
