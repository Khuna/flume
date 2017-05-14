package ru.svyaznoy.eventagent.utils.filereader;

import org.apache.commons.lang3.ArrayUtils;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class BufferedEventFinderFileReader {
    private Integer buffSize;
    private EventFinder eventFinder;
    private Long position;
    private Integer arrayPosition = 0;
    private String fileName;
    private RandomAccessFile fileReader = null;
    private FileChannel fileChannel = null;
    private byte[] byteArray;
    private List<FileStringRecord> lines = new ArrayList<FileStringRecord>();

    public BufferedEventFinderFileReader(String fileName, Long position, EventFinder eventFinder) throws IOException {
        this(fileName, position, eventFinder, 5000);
    }

    public BufferedEventFinderFileReader(String fileName, Long position, EventFinder eventFinder, Integer buffSize) throws IOException {
        this.fileName = fileName;
        this.position = position;
        this.eventFinder = eventFinder;
        this.buffSize = buffSize;
        byteArray = new byte[0];
        fileReader = new RandomAccessFile(new File(fileName), "r");
        fileChannel = fileReader.getChannel();
    }

    public Long getPosition() {
        return position;
    }

    public String getFileName() {
        return fileName;
    }

    public List<String> getEvent() throws IOException {
        if (lines.size() == 0) {
            if (getLines()) {
                return getEvent();
            } else {
                return null;
            }
        }
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).getLine();
            if (eventFinder.find(line)) {
                if (i != 0) {
                    return getResult(i);
                }
            }
        }
        if (getLines()) {
            return getEvent();
        }
        else {
            return getResult(lines.size());
        }
    }

    private List<String> getResult(Integer pos) {
        List<FileStringRecord> result;
        if (pos == lines.size()) {
            result = lines;
            lines = new ArrayList<FileStringRecord>();
        } else {
            result = lines.subList(0, pos);
            lines = lines.subList(pos, lines.size());
        }
        Integer addLength = FileStringRecord.getLength(result);
        position = position + addLength;
        arrayPosition = arrayPosition + addLength;
        byteArray = Arrays.copyOfRange(byteArray, addLength, byteArray.length);

        return FileStringRecord.getStringList(result);
    }

    private boolean getLines() throws IOException {
        byte[] bArray = readFromFileChannel();
        if (bArray == null) {
            return false;
        }
        if (bArray.length == 0) {
            return false;
        }
        addToByteArray(bArray);
        fromBytesToList(bArray.length);
        if (lines.size() > 0 ) {
            return true;
        } else {
            return false;
        }
    }

    private byte[] readFromFileChannel() throws IOException {
        int buff_size = buffSize <= fileChannel.size() - position - byteArray.length ? buffSize : (int)(fileChannel.size() - position - byteArray.length);
        if (buff_size <= 0) {
            return null;
        } else {
            ByteBuffer byteBuffer = ByteBuffer.allocate(buff_size);
            fileChannel.position(position + byteArray.length);
            fileChannel.read(byteBuffer);
            return byteBuffer.array();
        }
    }

    private void addToByteArray(byte[] addBytes) {
        if (byteArray == null || byteArray.length == 0) {
            byteArray = addBytes;
        } else if (addBytes == null || addBytes.length == 0) {
        } else {
            byte[] joinedArray = new byte[byteArray.length + addBytes.length];
            System.arraycopy(byteArray, 0, joinedArray, 0, byteArray.length);
            System.arraycopy(addBytes, 0, joinedArray, byteArray.length, addBytes.length);
            byteArray = joinedArray;
        }
    }

    private void fromBytesToList(Integer length) {
        int currPos = 0;
        lines = new ArrayList<FileStringRecord>();
        for (int i = 0; i < byteArray.length; i++) {
            if (byteArray[i] == 10) {
                String currentStr = null;
                try {
                    currentStr = new String(Arrays.copyOfRange(byteArray, currPos, i), "utf-8").replace("\n", "").replace("\r", "");
                } catch (UnsupportedEncodingException e) {
                    currentStr = new String(Arrays.copyOfRange(byteArray, currPos, i)).replace("\n", "").replace("\r", "");
                }
                if (currentStr.length() > 0) {
                    lines.add(new FileStringRecord(currentStr, i - currPos));
                    currPos = i;
                }
            }
        }
        if (currPos < byteArray.length) {
            lines.add(new FileStringRecord(new String(Arrays.copyOfRange(byteArray, currPos, byteArray.length)).replace("\n", "").replace("\r", ""), byteArray.length - currPos));
        }
    }

}




