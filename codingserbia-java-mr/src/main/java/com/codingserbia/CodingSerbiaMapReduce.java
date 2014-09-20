package com.codingserbia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codingserbia.writable.CustomerSessionWritable;

public class CodingSerbiaMapReduce extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(CodingSerbiaMapReduce.class);

    protected String customerCategoriesFilePath = "";
    protected String inputPath = "";
    protected String outputPath = "";

    public CodingSerbiaMapReduce(Configuration config) {
        super();
        setConf(config);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:/work/tools/hadoop-common-2.2.0-bin-master");

        Configuration config = new Configuration();
        CodingSerbiaMapReduce mr = new CodingSerbiaMapReduce(config);
        ToolRunner.run(config, mr, args);
    }

    protected boolean validateAndParseInput(String[] args) {
        if (args == null || args.length < 3) {
            LOGGER.error("Three arguments are required: path to customer categories file, path to input data and path to desired output directory.");
            return false;
        }

        if (args.length > 3) {
            LOGGER.error("Too many arguments. Only three arguments are required: path to customer categories file, path to input data and path to desired output directory.");
            return false;
        }

        customerCategoriesFilePath = args[0];
        LOGGER.info("Customer categories file path: " + customerCategoriesFilePath);
        getConf().set("customer.categories.file.path", customerCategoriesFilePath);

        inputPath = args[1];
        LOGGER.info("Input path: " + inputPath);

        outputPath = args[2];
        LOGGER.info("Output path: " + outputPath);

        LOGGER.info("Input validation succeeded");
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (!validateAndParseInput(args)) {
            throw new RuntimeException("Input validation failed.");
        }

        Job job = Job.getInstance(getConf());
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(CustomerSessionWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CustomerRecordsMapper.class);
        job.setReducerClass(CustomerRecordsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setJarByClass(CodingSerbiaMapReduce.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
