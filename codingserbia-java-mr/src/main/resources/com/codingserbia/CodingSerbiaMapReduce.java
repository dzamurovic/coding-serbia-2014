package com.codingserbia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

    protected String inputPath = "";
    protected String outputPath = "";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CodingSerbiaMapReduce(), args);
    }

    protected boolean validateAndParseInput(String[] args) {
        if (args == null || args.length < 2) {
            LOGGER.error("Two arguments are required: path to input data and path to desired output directory.");
            return false;
        }

        if (args.length > 2) {
            LOGGER.error("Too many arguments. Only two arguments are required: path to input data and path to desired output directory.");
            return false;
        }

        inputPath = args[0];
        LOGGER.info("Input path: " + inputPath);
        outputPath = args[1];
        LOGGER.info("Output path: " + outputPath);

        return true;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (!validateAndParseInput(args)) {
            System.exit(1);
        }

        LOGGER.info("Input validation succeeded");

        Job job = Job.getInstance(new Configuration());
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(CustomerSessionWritable.class);

        job.setOutputKeyClass(LongWritable.class);
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
