package com.codingserbia.datagenerator.json;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.codingserbia.datagenerator.DataSize;
import com.codingserbia.datagenerator.Generator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class outputs single JSON file.
 */
public class JsonDataGenerator implements Generator {

    private Random random;

    private ObjectMapper objectMapper;

    public JsonDataGenerator() {
        objectMapper = new ObjectMapper();
        random = new Random();
    }

    @Override
    public void generateData(DataSize dataSize) {
        try {
            FileWriter jsonFileWriter = new FileWriter("customer_records_" + dataSize.name() + ".json");

            for (long i = 0; i < dataSize.getSize(); i++) {
                CustomerSession customerSession = new CustomerSession();
                customerSession.sessionId = i + 1;
                customerSession.customerCategoryId = random.nextInt(13) + 1; // avoid zero value
                customerSession.products = pseudoPickProductsForCustomerSession();

                String customerSessionString = objectMapper.writeValueAsString(customerSession);
                jsonFileWriter.append(customerSessionString);
                if (i + 1 < dataSize.getSize()) {
                    jsonFileWriter.append("\n");
                }
            }

            jsonFileWriter.flush();
            jsonFileWriter.close();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<Product> pseudoPickProductsForCustomerSession() {
        int numberOfPseudoViews = random.nextInt(GeneratorData.products.length / 3);

        List<Product> sessionProducts = new ArrayList<Product>(numberOfPseudoViews);
        for (int i = 0; i < numberOfPseudoViews; i++) {
            int index = random.nextInt(GeneratorData.products.length);
            sessionProducts.add(new Product(GeneratorData.products[index]));
        }

        sessionProducts = pseudoBuyProductsForCustomerSession(sessionProducts);

        return sessionProducts;
    }

    private List<Product> pseudoBuyProductsForCustomerSession(List<Product> products) {
        List<Product> pickedProducts = products;

        int numberOfProducts = pickedProducts.size();
        if (numberOfProducts <= 0) {
            return pickedProducts;
        }

        int numberOfPseudoPurchases = random.nextInt(numberOfProducts);

        for (int i = 0; i < numberOfPseudoPurchases; i++) {
            int index = random.nextInt(numberOfProducts) - 1;
            if (index >= 0 && !pickedProducts.get(index).bought) {
                pickedProducts.get(index).bought = true;
                // We will just ignore the case when index is repeated by random number generator.
                // When that happens, number of products actually marked as bought will be smaller than number of pseudo purchases.
            }
        }

        return pickedProducts;
    }

}
