package com.codingserbia.dto;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomerSession {

    @JsonProperty
    public long customerCategoryId;

    @JsonProperty
    public List<Product> products;

    public CustomerSession() {
        products = new ArrayList<Product>();
    }
}
