package ru.svyaznoy.eventagent.logtech1c;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import ru.svyaznoy.eventagent.utils.Constants;
import ru.svyaznoy.eventagent.utils.filereader.BufferedEventFinderFileReader;
import ru.svyaznoy.eventagent.utils.filereader.PatternEventFinder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogTech1CManager {
    private BufferedEventFinderFileReader fileReader;
    private LogTech1CPropertiesManager propertiesManager;
    private Integer maxfieldsize;
    private Integer batchsize;
    private String systemName;
    private PatternEventFinder eventFinder;
    private String currStrDate = "";
    private String currHostName = "";
    private String currDirName = "";
    private static Pattern headerFinder = Pattern.compile(",([\\w\\:]+)=(?:'\\s*([^']*)'|\"\\s*([^\"]*)\"|([^'\"\\n\\r,]*))");

    public LogTech1CManager(Context context) throws IOException {
        batchsize = context.getInteger("batch-size", 10);
        maxfieldsize = context.getInteger("maxfieldsize", 999);
        propertiesManager = new LogTech1CPropertiesManager(context.getString("fileposition"), context.getString("directory"));
        eventFinder = new PatternEventFinder();
        eventFinder.setPattern(Pattern.compile("^\\d{2}:\\d{2}.\\d{4}"));
        systemName = context.getString("systemname");
    }

    public List<Event> getEvents(Integer size) throws IOException {
        LogTech1CFilePosition currFile = propertiesManager.getNextFile();
        List<Event> events = new ArrayList<Event>();
        if (currFile == null) {
            return events;
        }
        fileReader = getFileReader(currFile);
        int count = 0;
        Event event = null;
        while (count < size) {
            if (fileReader != null) {
                event = getEvent();
            } else {
                event = null;
            }
            if (event != null) {
                event.getHeaders().put("system_name", systemName);
                events.add(event);
                count++;
            } else {
                setPosition(currFile, fileReader);
                if (currFile.findNextFile()) {
                } else {
                    currFile = propertiesManager.getNextFile();
                    if (currFile == null) {
                        break;
                    }
                }
                if (currFile != null) {
                    try {
                        fileReader = getFileReader(currFile);
                    } catch (FileNotFoundException e) {
                        continue;
                    }
                } else {
                    return events;
                }
            }
        }
        setPosition(currFile, fileReader);
        return events;
    }

    private void setPosition(LogTech1CFilePosition position, BufferedEventFinderFileReader fileReader) {
        if (fileReader != null & position != null) {
            position.setPosition(fileReader.getPosition());
        }
    }

    private BufferedEventFinderFileReader getFileReader(LogTech1CFilePosition file) throws IOException {
        try {
            if (file != null) {
                getDateFromFileName(file.getFileName());
                getHostAndDirFromFileName(file.getFileName());
                return new BufferedEventFinderFileReader(file.getFileName(), file.getPosition(), eventFinder, 100000);
            } else {
                return null;
            }
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    public void savePropsFiles() throws IOException {
        propertiesManager.savePropsFiles();
    }

    private Event getEvent() {
        List<String> lines = null;
        try {
            lines = fileReader.getEvent();
        } catch (IOException e) {
            return null;
        }
        if (lines != null) {
            if (lines.size() > 0) {
                return parseLines(lines);
            }
        }
        return null;
    }

    private Event parseLines(List<String> lines) {
        Map<String, String > headers = new HashMap<String, String>();
        String str = null;
        for (String line : lines) {
            if (str == null) {
                str = line;
            } else if (!line.trim().equals("")){
                str = str + "\n" + line;
            }
        }

        if(str.startsWith("\uFEFF")) {
            str = str.substring(1);
        }
        if (str == null) {
            return null;
        }
        if (str.equals("")) {
            return null;
        }


        int pos = str.indexOf("-");
        if (pos == -1) {
            return null;
        }

        headers.put("time", str.substring(0,pos));
        str = str.substring(pos+1);

        headers.put("date", getDateTime(headers.get("time")));
        headers.put("hostname", getHostName());
        headers.put("directory", getDirName());

        pos = str.indexOf(",");
        headers.put("duration", str.substring(0,pos));
        str = str.substring(pos+1);

        pos = str.indexOf(",");
        headers.put("event", str.substring(0,pos));
        str = str.substring(pos+1);

        pos = str.indexOf(",");
        headers.put("position", str.substring(0,pos));
        str = str.substring(pos);

        try {
            while (!str.equals("")) {
                String[] header = getHeader(str);
                if (header == null) {
                    break;
                }
                if (header[1] == null) {
                    header = null;
                }
                if (header[1].length() > maxfieldsize) {
                    header[1] = header[1].substring(0, maxfieldsize) + " ...";
                }
                headers.put(header[0], header[1]);
                str = header[2];
            }
        } catch (Exception e) {
        }

        return EventBuilder.withBody("0".getBytes(Constants.TEXT_CHARSET), headers);
    }

    private String[] getHeader(String str) {
        Matcher matcher = headerFinder.matcher(str);
        String header[] = new String[3];
        int shift = 2;
        if (matcher.find()) {
            header[0] = matcher.group(1);
            if (matcher.group(4) != null) {
                header[1] = matcher.group(4);
            } else if (matcher.group(3) != null) {
                shift = 4;
                header[1] = matcher.group(3);
            } else if (matcher.group(2) != null) {
                shift = 4;
                header[1] = matcher.group(2);
            } else {
                return null;
            }
            header[2] = str.substring(header[0].length() + header[1].length() + shift);
        }
        return header;
    }

    public int getBatchSize() {
        return batchsize;
    }

    private void getDateFromFileName(String fileName) {
        String YYYY = "20" + fileName.substring(fileName.length()-12, fileName.length()-10);
        String MM = fileName.substring(fileName.length()-10, fileName.length()-8);
        String DD = fileName.substring(fileName.length()-8, fileName.length()-6);
        String HH = fileName.substring(fileName.length()-6, fileName.length()-4);

        currStrDate = new StringBuilder()
                .append(YYYY).append("-")
                .append(MM).append("-")
                .append(DD).append(" ")
                .append(HH).append(":")
                .toString();
    }

    private String getDateTime(String time) {
        String mm = time.substring(0, 2);
        String ss = time.substring(3, 5);
        String SSS;
        if (time.length() >= 10) {
            SSS = time.substring(6, 9);
        } else {
            SSS = "000";
        }

        return new StringBuilder()
                .append(currStrDate)
                .append(mm).append(":")
                .append(ss).append(".")
                .append(SSS)
                .toString();

    }

    private void getHostAndDirFromFileName(String fileName) {
        String host = fileName.substring(0, fileName.lastIndexOf("\\"));
        String dir = host.substring(0, host.lastIndexOf("\\"));
        currHostName = host.substring(host.lastIndexOf("\\")+1);
        currDirName = dir.substring(dir.lastIndexOf("\\")+1);
    }

    private String getHostName() {
        return currHostName;
    }

    private String getDirName() {
        return currDirName;
    }




}




