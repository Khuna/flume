package ru.svyaznoy.eventagent.log1c;

import java.lang.String;

public class Log1CIndexRecord {
    private String id;
    private String value;

    public Log1CIndexRecord(String id, String value) {
        this.id = id;
        this.value = value;
    }

    public String getID() {
        return id;
    }

    public String getValue() {
        return value;
    }
}
