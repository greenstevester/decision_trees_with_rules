package com.maxdemarzi.results;

public class StringResult {

    public static final StringResult EMPTY = new StringResult(null);

    public final String value;

    public StringResult(String value) {
        this.value = value;
    }
}
