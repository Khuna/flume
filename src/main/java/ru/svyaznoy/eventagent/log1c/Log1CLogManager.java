package ru.svyaznoy.eventagent.log1c;


import ru.svyaznoy.eventagent.utils.filereader.BufferedEventFinderFileReader;
import ru.svyaznoy.eventagent.utils.filereader.PatternEventFinder;
import java.io.File;
import java.io.IOException;
import java.lang.Boolean;import java.lang.Integer;import java.lang.String;import java.lang.StringBuilder;import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;import java.util.Date;import java.util.HashMap;import java.util.List;import java.util.Map;import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log1CLogManager {
    private Log1CPropertiesManager propertiesManager;
    private BufferedEventFinderFileReader fileReader;
    private Pattern pattern = Pattern.compile(",\\d{1,},");
    private PatternEventFinder eventFinder;
    private List<String> ignoreList;
    private List<String> lines;

    public Log1CLogManager(Log1CPropertiesManager propertiesManager) throws IOException {
        this.propertiesManager = propertiesManager;
        initialize();
    }

    private void initialize() throws IOException {
        eventFinder = new PatternEventFinder();
        eventFinder.setPattern(Pattern.compile("^[{]\\d{14},[NURC],"));
        fileReader = new BufferedEventFinderFileReader(propertiesManager.getLogFileName(), propertiesManager.getPosition(), eventFinder, 100000);
    }

    public Map<String,String> getEvent() throws IOException {
        lines = fileReader.getEvent();
        if (lines == null) {
            return null;
        }
        Map<String,String> event = parseList(lines);
        if (event != null) {
            return event;
        } else {
            return getEvent();
        }
    }

    public void setPropertiesManager() throws IOException {
        propertiesManager.setPosition(fileReader.getPosition());
        propertiesManager.setFileName(fileReader.getFileName());
        propertiesManager.savePropsFile();
    }

    public Boolean findNextFile() throws ParseException, IOException {
        SimpleDateFormat dateFormatInput = new SimpleDateFormat("yyyyMMddHHmmss");
        String oldLogFile = propertiesManager.getLogFileName();
        File path = new File(new File(oldLogFile).getParent());
        File[] files = path.listFiles();

        oldLogFile = new File(oldLogFile).getName();
        Date oldLogFileDate = dateFormatInput.parse(oldLogFile.substring(0, oldLogFile.length() - 4));
        String currentFile = "";
        Date currentFileDate = oldLogFileDate;

        if (files == null) {
            return null;
        }

        for (File file : files) {
            if (file.isDirectory() || !(file.getName().contains("lgp"))) {
                continue;
            }

            if (currentFile.equals("")) {
                String fileName = file.getName();
                if (!fileName.equals(oldLogFile)) {
                    Date fileNameDate = dateFormatInput.parse(fileName.substring(0, fileName.length() - 4));
                    if (fileNameDate.getTime() > oldLogFileDate.getTime()) {
                        currentFile = fileName;
                        currentFileDate = fileNameDate;
                    }
                }
            } else {
                String fileName = file.getName();
                if (!fileName.equals(oldLogFile)) {
                    Date fileNameDate = dateFormatInput.parse(fileName.substring(0, fileName.length() - 4));
                    if (fileNameDate.getTime() > oldLogFileDate.getTime() & fileNameDate.getTime() < currentFileDate.getTime()) {
                        currentFile = fileName;
                        currentFileDate = fileNameDate;
                    }
                }
            }
        }

        if (currentFile.equals("")) {
            return false;
        }else {
            String fileName = path.getPath() + "\\" + currentFile;
            propertiesManager.setFileName(fileName);
            propertiesManager.setPosition(0L);
            initialize();
            return true;
        }
    }

    private Map<String,String> parseList(List<String> lines) {
        if (!eventFinder.find(lines.get(0))) {
            return null;
        }

        Map<String,String> event= new HashMap<String,String>();
        String str = lines.get(0);
        String[] strArray = str.substring(1).split(",");
        event.put("date", strArray[0]);
        event.put("status", strArray[1]);

        str = lines.get(1);
        strArray = str.substring(1, str.indexOf("}")).split(",");
        event.put("seccount", strArray[0]);
        event.put("trannumber", strArray[1]);

        str = str.substring(str.indexOf("}")+2, str.indexOf(",\""));
        strArray = str.split(",");
        event.put("user", strArray[0]);
        event.put("computer", strArray[1]);
        event.put("application", strArray[2]);
        event.put("connection", strArray[3]);
        event.put("event", strArray[4]);
        event.put("importance", strArray[5]);

        //start find commentary
        str = lines.get(1).substring(lines.get(1).indexOf(",\"")+1);
        String commentary = "";
        Integer linesIndex = 1;
        while (true) {
            Integer pos;
            if (!str.substring(str.length() - 1).equals(",")) {
                pos = -1;
            } else {
                pos = str.substring(0, str.length() - 1).lastIndexOf(",");
            }
            if (pos > 0) {
                String marker = str.substring(pos);
                if (pattern.matcher(marker).find()) {
                    if (pos > 1) {
                        commentary = commentary + str.substring(1, pos-1);
                    }
                    break;
                } else {
                    linesIndex++;
                    str = lines.get(linesIndex);
                    commentary = commentary + str + "\n";
                }
            } else {
                commentary = commentary + str + "\n";
                linesIndex++;
                str = lines.get(linesIndex);
            }
        }
        event.put("commentary", commentary);
        //end find commentary

        str = lines.get(linesIndex);
        str = str.substring(0,str.length()-1);
        Integer pos = str.lastIndexOf(",");
        event.put("metadata", str.substring(pos+1));

        String data = "";
        while (true) {
            linesIndex++;
            str = lines.get(linesIndex);
            pos = str.indexOf("},");
            if (pos >= 0) {
                data = data + str.substring(0, pos+1);
                if (symbolCount(data, "[{]") == symbolCount(data, "[}]")) {
                    str = str.substring(pos + 2);
                    break;
                } else {
                    data = data + str.substring(pos+1);
                }

            } else {
                data = data + str;
            }
        }
        event.put("data" , data);

        pos = str.indexOf("\",");
        event.put("representation" , str.substring(1, pos));
        str = str.substring(pos+2, str.length()-1);
        strArray = str.split(",");
        event.put("server", strArray[0]);
        event.put("port", strArray[1]);
        event.put("additionalport", strArray[2]);
        event.put("session", strArray[3]);

        StringBuilder sBuilder = new StringBuilder();
        for (int i = 4; i < strArray.length; i++) {
            if (i < strArray.length-1) {
                sBuilder.append(strArray[i]).append(",");
            }else {
                sBuilder.append(strArray[i]);
            }
        }
        if (lines.get(linesIndex).substring(lines.get(linesIndex).length()-1).equals(",")) {
            sBuilder.append(",");
        }
        str = sBuilder.toString();
        String additionaldata = str;
        if (linesIndex < lines.size()-1) {
            linesIndex++;
            while (linesIndex < lines.size()) {
                additionaldata = additionaldata + "\n" + lines.get(linesIndex);
                linesIndex++;
            }
        }
        additionaldata = additionaldata.substring(0, additionaldata.lastIndexOf("}"));
        event.put("additionaldata", additionaldata);

        return event;
    }

    private Integer symbolCount(String str, String subStr) {
        Pattern pattern = Pattern.compile(subStr);
        Matcher matcher = pattern.matcher(str);
        int counter = 0;
        while (matcher.find()) {
            counter++;
        }
        return counter;
    }

    public void setIgnorelist(String exceptions) {
        ignoreList = new ArrayList<String>();
        if (!exceptions.isEmpty()) {
            String[] strs = exceptions.split(",");
            for (String str : strs) {
                if (!str.isEmpty()) {
                    ignoreList.add(str.trim());
                }
            }
        }
    }

    public Boolean hasIgnore(String eventType) {
        if (eventType == null) {
            return false;
        }
        if (ignoreList == null) {
            return false;
        }
        if (ignoreList.size() == 0) {
            return false;
        }

        for (String ignore : ignoreList) {
            if (eventType.contains(ignore)) {
                   return true;
            }
        }

        return false;
    }
}

