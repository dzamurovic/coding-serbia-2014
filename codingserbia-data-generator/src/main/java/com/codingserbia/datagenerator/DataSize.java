package com.codingserbia.datagenerator;

public enum DataSize {

    S(3);

    // S(150000);
    // M(7500000);
    // L(15000000);

    private DataSize(int size) {
        this.size = size;
    }

    private final int size;

    public int getSize() {
        return size;
    }

}
