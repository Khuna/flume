package ru.svyaznoy.eventagent.utils.filereader;

import java.util.ArrayList;
import java.util.List;

public class FileStringRecord {
    private String line;
    private Integer length;

    public String getLine() {
        return line;
    }

    public Integer getLength() {
        return length;
    }

    public FileStringRecord(String line, Integer length) {
        this.line = line;
        this.length = length;
    }

    public static Integer getLength(List<FileStringRecord> fileStringRecordList) {
        Integer length = 0;
        for (FileStringRecord fileStringRecord : fileStringRecordList) {
            length = length + fileStringRecord.length;
        }
        return length;
    }

    public static String getLines(List<FileStringRecord> fileStringRecordList) {
        String lines = null;
        for (FileStringRecord fileStringRecord : fileStringRecordList) {
            if (lines == null) {
                lines = fileStringRecord.line;
            } else {
                lines = "\n" + fileStringRecord.line;
            }
        }
        return lines;
    }

    public static List<String> getStringList(List<FileStringRecord> fileStringRecordList) {
        List<String> lines = new ArrayList<String>();
        for (FileStringRecord fileStringRecord : fileStringRecordList) {
            lines.add(fileStringRecord.line);
        }
        return lines;
    }
}
