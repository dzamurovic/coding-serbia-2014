package com.codingserbia;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class CodingSerbiaMapReduceTest {

    private CodingSerbiaMapReduce mr;

    @Before
    public void setUp() {
        mr = new CodingSerbiaMapReduce(new Configuration());
    }

    @Test
    public void testValidateAndParseInput() {
        String categoriesPath = "/some/path/categories.db";
        String inputPath = "/some/input/path";
        String outputPath = "/some/output/path";
        String unneededPath = "/some/unneeded/path";

        Assert.assertFalse(mr.validateAndParseInput(null));
        Assert.assertFalse(mr.validateAndParseInput(new String[] { categoriesPath }));
        Assert.assertFalse(mr.validateAndParseInput(new String[] { categoriesPath, inputPath }));
        Assert.assertFalse(mr
                        .validateAndParseInput(new String[] { categoriesPath, inputPath, outputPath, unneededPath }));

        Assert.assertTrue(mr.validateAndParseInput(new String[] { categoriesPath, inputPath, outputPath }));
        Assert.assertEquals(categoriesPath, mr.customerCategoriesFilePath);
        Assert.assertEquals(inputPath, mr.inputPath);
        Assert.assertEquals(outputPath, mr.outputPath);
    }

}
