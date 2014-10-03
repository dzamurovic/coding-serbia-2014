package com.codingserbia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.codingserbia.dto.CustomerSession;
import com.codingserbia.dto.Product;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonLoader extends LoadFunc implements LoadMetadata {

    private ObjectMapper jsonMapper;

    private RecordReader reader;

    public JsonLoader() {
        jsonMapper = new ObjectMapper();
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple = null;

        try {
            if (reader.nextKeyValue()) {
                tuple = TupleFactory.getInstance().newTuple(4);

                Text value = (Text) reader.getCurrentValue();
                CustomerSession jsonObj = jsonMapper.readValue(value.toString(), CustomerSession.class);
                tuple.set(0, jsonObj.sessionId);
                tuple.set(1, jsonObj.customerCategoryId);
                tuple.set(2, jsonObj.customerCategoryDescription);

                List<Tuple> productList = new ArrayList<Tuple>();
                for (Product p : jsonObj.products) {
                    Tuple productTuple = TupleFactory.getInstance().newTuple(5);
                    productTuple.set(0, p.id);
                    productTuple.set(1, p.name);
                    productTuple.set(2, p.category);
                    productTuple.set(3, p.bought ? 1 : 0);
                    productTuple.set(4, p.price);
                    productList.add(productTuple);
                }
                DataBag productBag = BagFactory.getInstance().newDefaultBag(productList);
                tuple.set(3, productBag);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return tuple;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        Schema schema = new Schema();

        FieldSchema sessionId = new FieldSchema("sessionId", DataType.LONG);
        schema.add(sessionId);

        FieldSchema customerGroupId = new FieldSchema("customerCategoryId", DataType.LONG);
        schema.add(customerGroupId);

        FieldSchema customerGroupDescription = new FieldSchema("customerCategoryDescription", DataType.CHARARRAY);
        schema.add(customerGroupDescription);

        FieldSchema products = new FieldSchema("products", DataType.BAG);
        FieldSchema arraySchema = new FieldSchema("ARRAY_ELEM", DataType.TUPLE);
        arraySchema.schema = new Schema();
        arraySchema.schema.add(new FieldSchema("productId", DataType.LONG));
        arraySchema.schema.add(new FieldSchema("productName", DataType.CHARARRAY));
        arraySchema.schema.add(new FieldSchema("productCategoryId", DataType.LONG));
        arraySchema.schema.add(new FieldSchema("bought", DataType.INTEGER));
        arraySchema.schema.add(new FieldSchema("price", DataType.DOUBLE));
        products.schema = new Schema(arraySchema);

        schema.add(products);

        return new ResourceSchema(schema);
    }

    @Override
    public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setPartitionFilter(Expression arg0) throws IOException {
        // TODO Auto-generated method stub

    }

}
