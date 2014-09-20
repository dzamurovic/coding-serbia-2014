package com.codingserbia.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class CustomerSessionWritablesGroupedByCustomerCategoryId implements Writable {

    public LongWritable customerCategoryId;

    public MapWritable sessions;

    public CustomerSessionWritablesGroupedByCustomerCategoryId() {
        super();
        customerCategoryId = new LongWritable();
        sessions = new MapWritable();
    }

    public CustomerSessionWritablesGroupedByCustomerCategoryId(Long categoryId) {
        super();
        customerCategoryId = new LongWritable(categoryId);
        sessions = new MapWritable();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        customerCategoryId.readFields(input);
        sessions.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        customerCategoryId.write(output);
        sessions.write(output);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((customerCategoryId == null) ? 0 : customerCategoryId.hashCode());
        result = prime * result + ((sessions == null) ? 0 : sessions.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CustomerSessionWritablesGroupedByCustomerCategoryId other = (CustomerSessionWritablesGroupedByCustomerCategoryId) obj;
        if (customerCategoryId == null) {
            if (other.customerCategoryId != null) {
                return false;
            }
        } else if (!customerCategoryId.equals(other.customerCategoryId)) {
            return false;
        }
        if (sessions == null) {
            if (other.sessions != null) {
                return false;
            }
        } else if (!sessions.equals(other.sessions)) {
            return false;
        }
        return true;
    }

}
