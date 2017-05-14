package ru.svyaznoy.eventagent.utils;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;

public class Utils {
    public static String ListToString(List<String> list) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean firstTime = true;
        for (String element : list) {
            if (firstTime) {
                stringBuilder.append(element);
                firstTime = false;
            } else if (!element.trim().equals("")){
                stringBuilder.append("\n").append(element);
            }
        }
        return stringBuilder.toString();
    }

    public static String parseSQLIDTo1CID(String string) {
        return new StringBuilder()
                .append(string.substring(28)).append("-")
                .append(string.substring(24,28)).append("-")
                .append(string.substring(19,23)).append("-")
                .append(string.substring(6,8)).append(string.substring(4,6)).append("-")
                .append(string.substring(2,4)).append(string.substring(0,2))
                .append(string.substring(11,13)).append(string.substring(9,11))
                .append(string.substring(16,18)).append(string.substring(14,16))
                .toString();
    }

    public static Timestamp getTimestamp() {
        return new Timestamp(Calendar.getInstance().getTime().getTime());
    }

    public static String mapToJson(Map<String, String> map) {
        return JSONValue.toJSONString(map);
    }
}
