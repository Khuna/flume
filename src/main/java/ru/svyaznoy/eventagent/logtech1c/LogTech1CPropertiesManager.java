package ru.svyaznoy.eventagent.logtech1c;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogTech1CPropertiesManager {
    private String directory;
    private String propsFileName;
    private List<String> directories;
    private List<LogTech1CFilePosition> positions;
    private int currpos = -1;

    public LogTech1CPropertiesManager(String propsFileName, String directory) throws IOException {
        this.directory = directory;
        this.propsFileName = propsFileName;
        initialize();
    }

    public void initialize() throws IOException {
        positions = new ArrayList<LogTech1CFilePosition>();
        FileReader fileReader = new FileReader(propsFileName);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();
        while (line != null) {
            try {
                LogTech1CFilePosition position = new LogTech1CFilePosition(line);
                positions.add(position);
                line = bufferedReader.readLine();
            } catch (Exception e) {
                line = null;
            }
        }

        directories = Arrays.asList(directory.split(","));
        for (String directory : directories) {
            directory = directory.replace("/", "\\");
            readDirectory(directory.trim());
        }

        for (int i = positions.size()-1; i > 0  ; i--) {
            LogTech1CFilePosition position = positions.get(i);
            if (!position.getExist()) {
                positions.remove(position);
            }
        }

        savePropsFiles();
    }

    private void readDirectory(String directory) {
        File dir = new File(directory);
        for (String underdir : dir.list()) {
            underdir = directory + "\\" + underdir;
            LogTech1CFilePosition position = alreadeReadedDirectory(underdir);
            if (position != null) {
                position.setExist(true);
            }else {
                String firstFile = getFirstFile(underdir);
                if (firstFile != null) {
                    positions.add(new LogTech1CFilePosition(underdir + "\\" + firstFile, 0, true));
                }
            }
        }
    }

    private String getFirstFile(String directory) {
        String filename = null;
        File dir = new File(directory);
        for (String underdir : dir.list()) {
            if (filename == null) {
                filename = underdir;
            }
            if (LogTech1CFilePosition.getNumber(filename) > LogTech1CFilePosition.getNumber(underdir)) {
                filename = underdir;
            }
        }
        return filename;
    }

    private LogTech1CFilePosition alreadeReadedDirectory(String directory) {
        for (LogTech1CFilePosition filepos : positions) {
            if (filepos.getDirectory().equals(directory.toLowerCase())) {
                return filepos;
            }
        }
        return null;
    }

    public void savePropsFiles() throws IOException {
        FileWriter fileWriter = new FileWriter(propsFileName);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        for (LogTech1CFilePosition position : positions) {
            bufferedWriter.write(position.toString());
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
    }

    public List<LogTech1CFilePosition> getPositions() {
        return positions;
    }

    public void setPosition(List<LogTech1CFilePosition> positions) {
        this.positions = positions;
    }

    public LogTech1CFilePosition getNextFile() {
        currpos++;
        if (currpos < positions.size()) {
            return positions.get(currpos);
        } else {
            return null;
        }
    }
}
