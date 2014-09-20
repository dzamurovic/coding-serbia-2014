package com.codingserbia.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustomerCategoryWritable implements Writable {

    public LongWritable categoryId;
    public Text description;
    public Text gender;

    public CustomerCategoryWritable() {
        super();
        categoryId = new LongWritable();
        description = new Text();
        gender = new Text();
    }

    public CustomerCategoryWritable(long id, String description, String gender) {
        super();
        categoryId = new LongWritable(id);
        this.description = new Text(description);
        this.gender = new Text(gender);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        categoryId.readFields(input);
        description.readFields(input);
        gender.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        categoryId.write(output);
        description.write(output);
        gender.write(output);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((categoryId == null) ? 0 : categoryId.hashCode());
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + ((gender == null) ? 0 : gender.hashCode());
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
        CustomerCategoryWritable other = (CustomerCategoryWritable) obj;
        if (categoryId == null) {
            if (other.categoryId != null) {
                return false;
            }
        } else if (!categoryId.equals(other.categoryId)) {
            return false;
        }
        if (description == null) {
            if (other.description != null) {
                return false;
            }
        } else if (!description.equals(other.description)) {
            return false;
        }
        if (gender == null) {
            if (other.gender != null) {
                return false;
            }
        } else if (!gender.equals(other.gender)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "CustomerCategoryWritable [categoryId=" + categoryId + ", description=" + description + ", gender=" + gender + "]";
    }

}
