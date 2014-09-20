package com.codingserbia.dto;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomerSessionOutput {

    @JsonProperty
    public long customerCategoryId;

    @JsonProperty
    public String customerCategoryDescription;

    @JsonProperty
    public List<ProductOutput> products;

    @JsonProperty
    public float averageNumberOfViews;

    @JsonProperty
    public float averageNumberOfPurchases;

    @JsonProperty
    public float totalPurchase;

    public CustomerSessionOutput() {
        products = new ArrayList<ProductOutput>();
    }
}
