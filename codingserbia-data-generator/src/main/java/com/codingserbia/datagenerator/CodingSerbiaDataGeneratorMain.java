package com.codingserbia.datagenerator;

import com.codingserbia.datagenerator.json.JsonDataGenerator;

public class CodingSerbiaDataGeneratorMain {

    public static void main(String[] args) {
        CodingSerbiaDataGeneratorMain main = new CodingSerbiaDataGeneratorMain();
        main.invokeJsonDataGeneration();
    }

    private void invokeJsonDataGeneration() {
        Generator generator = new JsonDataGenerator();
        for (DataSize dataSize : DataSize.values()) {
            generator.generateData(dataSize);
        }
    }

}
