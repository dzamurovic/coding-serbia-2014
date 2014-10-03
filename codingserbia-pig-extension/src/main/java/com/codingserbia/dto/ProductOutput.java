package com.codingserbia.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductOutput {

    @JsonProperty
    public long id;

    @JsonProperty
    public String name;

    public ProductOutput() {
        name = "";
    }

    public ProductOutput(long id, String name) {
        super();
        this.id = id;
        this.name = name;
    }

}
