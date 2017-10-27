package com.mapred.dto;

public class KeyValue {

    private double value;
    private String key;

    public KeyValue() {
    }


    public KeyValue(double value, String key) {
        this.value = value;
        this.key = key;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
