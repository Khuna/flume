package ru.svyaznoy.eventagent.log1c;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import ru.svyaznoy.eventagent.utils.Constants;
import ru.svyaznoy.eventagent.utils.filereader.BufferedEventFinderFileReader;
import ru.svyaznoy.eventagent.utils.filereader.PatternEventFinder;

import java.io.*;
import java.lang.Exception;import java.lang.Integer;import java.lang.String;import java.util.*;
import java.util.regex.Pattern;

public class Log1CIndexManager {

    private String fileName;
    private BufferedEventFinderFileReader fileReader;
    private static Map<Integer, Map<Integer, Log1CIndexRecord>> indexData = new HashMap<Integer, Map<Integer, Log1CIndexRecord>>();

    public String getValue(Integer key1, Integer key2) throws Exception {
        if (key2 ==0) {
            return "";
        }
        if (indexData.keySet().contains(key1)) {
            if (indexData.get(key1).keySet().contains(key2)) {
                return indexData.get(key1).get(key2).getValue();
            }
        }
        return null;
    }

    public String getID(Integer key1, Integer key2) throws Exception {
        if (key2 ==0) {
            return "";
        }
        if (indexData.keySet().contains(key1)) {
            if (indexData.get(key1).keySet().contains(key2)) {
                return indexData.get(key1).get(key2).getID();
            }
        }
        return null;
    }

    public Log1CIndexManager(String fileName) throws Exception {
        indexData.put(1, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(2, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(3, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(4, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(5, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(6, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(7, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(8, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(11, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(12, new HashMap<Integer, Log1CIndexRecord>());
        indexData.put(13, new HashMap<Integer, Log1CIndexRecord>());

        this.fileName = fileName;
        iniatialize();
        readIndex();
    }

    private void iniatialize() throws IOException {
        PatternEventFinder eventFinder = new PatternEventFinder();
        eventFinder.setPattern(Pattern.compile("^[{][1-9]{1},|[{]11,|[{]12|[{]13,"));
        fileReader = new BufferedEventFinderFileReader(fileName, 0L, eventFinder, 100000);
    }

    private void readIndex() throws Exception {
        List<String> lines = null;
        while (true) {
            lines = fileReader.getEvent();
            if (lines == null) {
                break;
            }
            parseLines(lines);
        }
    }

    private void parseLines(List<String> lines) throws Exception {
        String str = lines.get(0).substring(0,3);
        if (str.equals("{1,")) {
            parse4ParametrString(lines, 1);
        } else if (str.equals("{2,")) {
            parse3ParametrString(lines, 2);
        } else if (str.equals("{3,")) {
            parse3ParametrString(lines, 3);
        } else if (str.equals("{4,")) {
            parse3ParametrString(lines, 4);
        } else if (str.equals("{5,")) {
            parse4ParametrString(lines, 5);
        } else if (str.equals("{6,")) {
            parse3ParametrString(lines, 6);
        } else if (str.equals("{7,")) {
            parse3ParametrString(lines, 7);
        } else if (str.equals("{8,")) {
            parse3ParametrString(lines, 8);
//        } else if (str.equals("{11")) {
//            parseUndefinedFormatString(lines, 11);
//        } else if (str.equals("{12")) {
//            parseUndefinedFormatString(lines, 12);
//        } else if (str.equals("{13")) {
//            parseUndefinedFormatString(lines, 13);
        } else {
        }
    }

    private void parse3ParametrString(List<String> lines, Integer indexType) {
        String[] strArray = lines.get(0).replaceAll("[{]", "").replaceAll("[}]", "").replaceAll("\"", "").split(",");
        Log1CIndexRecord indexRecord = new Log1CIndexRecord(null, strArray[1]);
        indexData.get(indexType).put(Integer.parseInt(strArray[2]), indexRecord);
    }

    private void parse4ParametrString(List<String> lines, Integer indexType) {
        String[] strArray = lines.get(0).replaceAll("[{]", "").replaceAll("[}]", "").replaceAll("\"", "").split(",");
        Log1CIndexRecord indexRecord = new Log1CIndexRecord(strArray[1], strArray[2]);
        indexData.get(indexType).put(Integer.parseInt(strArray[3]), indexRecord);
    }

    //TODO
    private void parseUndefinedFormatString(List<String> lines, Integer indexType) {

    }

    public Event toFlumeEvent(Map<String,String> map, Integer maxsize, Log1CLogManager logManager) throws Exception {
        Map<String, String> headers = new HashMap<String, String>();

        headers.put("event", getValue(4, Integer.parseInt(map.get("event"))));
        if (logManager.hasIgnore(headers.get("event"))) {
            return null;
        }
        headers.put("data", map.get("date"));
        headers.put("status", map.get("status"));
        headers.put("seccount", map.get("seccount"));
        headers.put("trannumber", map.get("trannumber"));

        headers.put("userid", getID(1, Integer.parseInt(map.get("user"))));
        headers.put("username", getValue(1, Integer.parseInt(map.get("user"))));

        headers.put("computer", getValue(2, Integer.parseInt(map.get("computer"))));
        headers.put("application", getValue(3, Integer.parseInt(map.get("application"))));
        headers.put("connection", map.get("connection"));
        headers.put("importance", map.get("importance"));

        String commentary = map.get("commentary");
        if (commentary.length() > maxsize) {
            commentary  = "trimmed" + "\n" + commentary.substring(0, maxsize);
        }
        headers.put("commentary", commentary);

        headers.put("matadataid", getID(5, Integer.parseInt(map.get("user"))));
        headers.put("matadataname", getValue(5, Integer.parseInt(map.get("user"))));

        headers.put("data", map.get("data"));
        headers.put("representation", map.get("representation"));
        headers.put("server", getValue(6, Integer.parseInt(map.get("server"))));
        headers.put("port", getValue(7, Integer.parseInt(map.get("port"))));
        headers.put("additionalport", getValue(8, Integer.parseInt(map.get("additionalport"))));
        headers.put("session", map.get("session"));
        headers.put("additionaldata", map.get("additionaldata"));

        for (String key : headers.keySet()) {
            if (headers.get(key) == null) {
                headers.put(key, "null");
            }
        }

        return EventBuilder.withBody("".getBytes(Constants.TEXT_CHARSET), headers);
    }

}