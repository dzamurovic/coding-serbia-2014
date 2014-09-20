package com.codingserbia.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Product {

    @JsonProperty
    public int id;

    @JsonProperty
    public String name;

    @JsonProperty
    public String category;

    @JsonProperty
    public boolean bought;

    @JsonProperty
    public double price;

    @JsonIgnore
    public int numberOfPurschases;

    public Product() {

    }

    public Product(int id, String name, String category, boolean bought, double price) {
        super();
        this.id = id;
        this.name = name;
        this.category = category;
        this.bought = bought;
        this.price = price;
    }

    public Product(Product aProduct) {
        super();
        this.id = aProduct.id;
        this.name = aProduct.name;
        this.category = aProduct.category;
        this.bought = aProduct.bought;
        this.price = aProduct.price;
    }

}
