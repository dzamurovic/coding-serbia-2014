package com.codingserbia.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.codingserbia.dto.CustomerSession;

public class CustomerSessionWritable implements Writable {

    public LongWritable categoryId;

    public Text categoryDescription;

    public ProductArrayWritable products;

    public CustomerSessionWritable() {
        super();
        categoryId = new LongWritable();
        categoryDescription = new Text();
        products = new ProductArrayWritable(ProductWritable.class);
    }

    public CustomerSessionWritable(String categoryDesc, CustomerSession json) {
        super();
        categoryId = new LongWritable(json.customerCategoryId);
        categoryDescription = new Text(categoryDesc);

        products = new ProductArrayWritable(ProductWritable.class);
        ProductWritable[] pwArray = new ProductWritable[json.products.size()];
        for (int i = 0; i < json.products.size(); i++) {
            ProductWritable pw = new ProductWritable(json.products.get(i));
            pwArray[i] = pw;
        }
        products.set(pwArray);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        categoryId.readFields(input);
        categoryDescription.readFields(input);
        products.readFields(input);
    }

    @Override
    public void write(DataOutput ouput) throws IOException {
        categoryId.write(ouput);
        categoryDescription.write(ouput);
        products.write(ouput);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((categoryDescription == null) ? 0 : categoryDescription.hashCode());
        result = prime * result + ((categoryId == null) ? 0 : categoryId.hashCode());
        result = prime * result + ((products == null) ? 0 : products.hashCode());
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
        CustomerSessionWritable other = (CustomerSessionWritable) obj;
        if (categoryDescription == null) {
            if (other.categoryDescription != null) {
                return false;
            }
        } else if (!categoryDescription.equals(other.categoryDescription)) {
            return false;
        }
        if (categoryId == null) {
            if (other.categoryId != null) {
                return false;
            }
        } else if (!categoryId.equals(other.categoryId)) {
            return false;
        }
        if (products == null) {
            if (other.products != null) {
                return false;
            }
        } else if (!products.equals(other.products)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "CustomerSessionWritable [categoryId=" + categoryId + ", categoryDescription="
                        + categoryDescription.toString() + ", products=[" + products.toString() + "]]";
    }
}
