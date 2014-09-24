package de.codecentric;

import java.io.File;
import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

public class PigAggregationTest {

    private static final String TEST_PATH = "src/test/resources/";

    private static PigTest test;

    @BeforeClass
    public static void setUp() throws IOException, ParseException {
        test = new PigTest("src/main/resources/pig_with_2_outputs_short.pig");
        test.override("products",
                        "products = LOAD '"
                                        + TEST_PATH
                                        + "input/customer_records_map_reduce_input.json' USING JsonLoader('sessionId:int,customerCategoryId:int,products:{(id:int,name:chararray,category:chararray,bought:boolean,price:double)}');");
        test.override("categories", "categories = LOAD '" + TEST_PATH
                        + "input/customer_categories.db' AS (categoryId:int,age:chararray,gender:chararray);");
    }

    @Test
    public void testTopFiveProducts() throws IOException, ParseException {
        test.assertOutput("resultTopFiveProducts", new File(TEST_PATH + "results/resultTopFiveProducts.txt"));
    }

    @Test
    public void testAverageSeenProducts() throws IOException, ParseException {
        test.assertOutput("averageCountedProducts", new File(TEST_PATH + "results/averageSeenProducts.txt"));
    }

    @Test
    public void testAverageBoughtProducts() throws IOException, ParseException {
        test.assertOutput("resultAverageBoughtProducts", new File(TEST_PATH + "results/averageBoughtProducts.txt"));
    }

    @Test
    public void testAverageSpent() throws IOException, ParseException {
        test.assertOutput("averagePrice", new File(TEST_PATH + "results/averagePrice.txt"));
    }

}
