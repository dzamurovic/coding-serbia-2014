package com.codingserbia.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.codingserbia.dto.Product;

public class ProductWritable implements Writable {

    public LongWritable id;

    public Text name;

    public Text category;

    public BooleanWritable bought;

    public DoubleWritable price;

    public ProductWritable() {
        super();
        id = new LongWritable();
        name = new Text();
        category = new Text();
        bought = new BooleanWritable();
        price = new DoubleWritable();
    }

    public ProductWritable(Product json) {
        super();
        id = new LongWritable(json.id);
        name = new Text(json.name);
        category = new Text(json.category);
        bought = new BooleanWritable(json.bought);
        price = new DoubleWritable(json.price);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        id.readFields(input);
        name.readFields(input);
        category.readFields(input);
        bought.readFields(input);
        price.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        id.write(output);
        name.write(output);
        category.write(output);
        bought.write(output);
        price.write(output);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bought == null) ? 0 : bought.hashCode());
        result = prime * result + ((category == null) ? 0 : category.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((price == null) ? 0 : price.hashCode());
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
        ProductWritable other = (ProductWritable) obj;
        if (bought == null) {
            if (other.bought != null) {
                return false;
            }
        } else if (!bought.equals(other.bought)) {
            return false;
        }
        if (category == null) {
            if (other.category != null) {
                return false;
            }
        } else if (!category.equals(other.category)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (price == null) {
            if (other.price != null) {
                return false;
            }
        } else if (!price.equals(other.price)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "ProductWritable [id=" + id + ", name=" + name + ", category=" + category + ", bought=" + bought + ", price=" + price + "]";
    }

}
