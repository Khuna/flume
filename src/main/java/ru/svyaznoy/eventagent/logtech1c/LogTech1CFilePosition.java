package ru.svyaznoy.eventagent.logtech1c;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;


public class LogTech1CFilePosition {
    private String filename;
    private long position;
    private boolean exist;


    public LogTech1CFilePosition(String unparsedstring) throws Exception {
        String[] lines = unparsedstring.replaceAll(" ", "").split(",");
        if (lines.length == 2) {
            filename = lines[0];
            position = Long.parseLong(lines[1]);
            exist = false;
        } else {
            throw new Exception("cant parse file position");
        }
    }

    public LogTech1CFilePosition(String filename, long position) {
        this(filename, position, false);
    }

    public LogTech1CFilePosition(String filename, long position, boolean exist) {
        this.filename = filename;
        this.position = position;
        this.exist = exist;
    }

    public String getFileName() {
        return filename;
    }

    public String getDirectory() {
        File file = new File(filename);
        return file.getParent().toLowerCase();
    }

    public long getPosition() {
        return position;
    }

    public void setFileName(String filename) {
        this.filename = filename;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return filename + ", " + position;
    }

    public boolean findNextFile() {
        File file = new File(filename);

        int filenamenum = getNumber(file.getName());
        int currfilenum = 99999999;
        String currfilename = "";

        File directory = new File(file.getParent());
        for (String under : directory.list()) {
            int undernum = getNumber(under);
            if (undernum != -1 & undernum > filenamenum & undernum < currfilenum) {
                currfilename = under;
                currfilenum = undernum;
            }
        }

        if (!currfilename.equals(file.getName()) & !currfilename.equals("")) {
            filename = file.getParent() + "\\" + currfilename;
            position = 0;
            return true;
        } else {
            return false;
        }
    }

    public static int getNumber(String filename) {
        if (filename == null) {
            return -1;
        }
        try {
            int num = Integer.parseInt(filename.substring(0, 2)) * 1000000;
            num = num + Integer.parseInt(filename.substring(2, 4)) * 10000;
            num = num + Integer.parseInt(filename.substring(4, 6)) * 100;
            num = num + Integer.parseInt(filename.substring(6, 8));
            return num;
        } catch (Exception e) {
            return -1;
        }
    }

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public boolean getExist() {
        return exist;
    }
}
