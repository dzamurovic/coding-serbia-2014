package com.codingserbia.dto;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomerSessionOutput {

    @JsonProperty
    public long customerCategoryId;

    @JsonProperty(required = false)
    public String customerCategoryDescription;

    @JsonProperty
    public List<ProductOutput> products;

    public CustomerSessionOutput() {
        products = new ArrayList<ProductOutput>();
    }
}
