package com.codingserbia;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codingserbia.dto.CustomerSession;
import com.codingserbia.writable.CustomerCategoryWritable;
import com.codingserbia.writable.CustomerSessionWritable;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomerRecordsMapper extends Mapper<LongWritable, Text, LongWritable, CustomerSessionWritable> {

    private static Logger LOGGER = LoggerFactory.getLogger(CustomerRecordsMapper.class);

    private Map<LongWritable, CustomerCategoryWritable> groupedCategories;

    private ObjectMapper jsonMapper;

    public CustomerRecordsMapper() {
        super();
        groupedCategories = new HashMap<LongWritable, CustomerCategoryWritable>();
    }

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);

        jsonMapper = new ObjectMapper();

        String dimDataPath = context.getConfiguration().get("customer.categories.file.path");
        loadCustomerCategories(dimDataPath);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            CustomerSession jsonObj = jsonMapper.readValue(value.toString(), CustomerSession.class);
            CustomerSessionWritable session = new CustomerSessionWritable(jsonObj);

            LongWritable categoryId = session.categoryId;
            context.write(categoryId, session);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void loadCustomerCategories(String path) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(path.replace("file://", ""))));
            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                long categoryId = Long.valueOf(columns[0]);
                String description = columns[1];
                String gender = columns[2];
                CustomerCategoryWritable writable = new CustomerCategoryWritable(categoryId, description, gender);
                groupedCategories.put(writable.categoryId, writable);
            }
            br.close();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

}
