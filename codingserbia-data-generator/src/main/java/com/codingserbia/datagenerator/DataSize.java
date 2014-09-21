package com.codingserbia.datagenerator;

public enum DataSize {

    S(3);

    // S(150000);
    // M(750000);
    // L(1500000);
    // XL(2250000);

    private DataSize(int size) {
        this.size = size;
    }

    private final int size;

    public int getSize() {
        return size;
    }

}
