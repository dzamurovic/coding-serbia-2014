package com.codingserbia.datagenerator;

public enum DataSize {

    S(3);

    // S(100000);
    // M(10000000);
    // L(50000000);

    private DataSize(int size) {
        this.size = size;
    }

    private final int size;

    public int getSize() {
        return size;
    }

}
