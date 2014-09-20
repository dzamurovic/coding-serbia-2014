package com.codingserbia;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codingserbia.dto.CustomerCategoryProductBag;
import com.codingserbia.dto.CustomerSessionOutput;
import com.codingserbia.dto.ProductOutput;
import com.codingserbia.writable.CustomerSessionWritable;
import com.codingserbia.writable.ProductWritable;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomerRecordsReducer extends Reducer<LongWritable, CustomerSessionWritable, NullWritable, Text> {

    private static Logger LOGGER = LoggerFactory.getLogger(CustomerRecordsReducer.class);

    private Map<LongWritable, CustomerCategoryProductBag> categoryMap;

    private ObjectMapper jsonMapper;

    public CustomerRecordsReducer() {
        super();
        categoryMap = new HashMap<LongWritable, CustomerCategoryProductBag>();
        jsonMapper = new ObjectMapper();
    }

    @Override
    protected void reduce(LongWritable key, Iterable<CustomerSessionWritable> values, Context context)
                    throws IOException, InterruptedException {
        CustomerCategoryProductBag aBag = categoryMap.get(key);
        if (aBag == null) {
            aBag = new CustomerCategoryProductBag();
            aBag.customerCategoryId = key;
        }

        for (CustomerSessionWritable value : values) {
            aBag.increaseNumberOfSessions();
            if (aBag.customerCategoryDescription.getLength() == 0) {
                aBag.customerCategoryDescription = value.categoryDescription;
            }

            Writable[] productWritables = value.products.get();

            for (Writable writable : productWritables) {
                ProductWritable product = (ProductWritable) writable;

                if (!aBag.contains(product.id)) {
                    aBag.add(product);
                } else {
                    aBag.processOccurance(product);
                }
            }
        }

        categoryMap.put(key, aBag);

        int numberOfTopBoughtProducts = 5;
        List<ProductWritable> topProducts = aBag.getTopProductsBought(numberOfTopBoughtProducts);

        CustomerSessionOutput outputJsonObj = new CustomerSessionOutput();
        outputJsonObj.customerCategoryId = key.get();
        outputJsonObj.customerCategoryDescription = aBag.customerCategoryDescription.toString();
        outputJsonObj.averageNumberOfViews = aBag.calculateAverageNumberOfViews();
        outputJsonObj.averageNumberOfPurchases = aBag.calculateAverageNumberOfPurchases();
        outputJsonObj.averagePurchase = aBag.calculateAveragePurchase();
        for (ProductWritable pw : topProducts) {
            outputJsonObj.products.add(new ProductOutput(pw.id.get(), pw.name.toString()));
        }

        context.write(NullWritable.get(), new Text(jsonMapper.writeValueAsString(outputJsonObj)));
        LOGGER.info(jsonMapper.writeValueAsString(outputJsonObj));
    }
}
