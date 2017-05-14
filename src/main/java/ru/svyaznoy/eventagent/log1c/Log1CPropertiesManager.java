package ru.svyaznoy.eventagent.log1c;

import java.io.*;
import java.lang.Long;
import java.lang.String;

public class Log1CPropertiesManager {
    private String propsFileName;
    private String logFileName;
    private Long position;

    public Log1CPropertiesManager(String propsFileName) throws IOException {
        this.propsFileName = propsFileName;
        initialize();
    }

    private void initialize() throws IOException{
    	BufferedReader bufferedReader = null;
    	try {

	    	bufferedReader = new BufferedReader(new FileReader(propsFileName));
	        String line = bufferedReader.readLine();
	        while (line != null) {
	            if (line.contains("logfilename")) {
	                logFileName = line.substring(14);
	            }
	            if (line.contains("position")) {
	                position = Long.parseLong(line.substring(10).replace(" ", ""));
	            }
	            line = bufferedReader.readLine();
	        }
    	}
        finally {
        	if (bufferedReader != null)
        		bufferedReader.close();
        }
    }

    public void savePropsFile() throws IOException {
    	BufferedWriter bufferedWriter = null;
    	
    	try {
	    	bufferedWriter = new BufferedWriter(new FileWriter(propsFileName, false));
	        bufferedWriter.write("logfilename = " + logFileName);
	        bufferedWriter.newLine();
	        bufferedWriter.write("position = " + position);
	        bufferedWriter.flush();
    	}
    	finally {
    		if (bufferedWriter != null)
    			bufferedWriter.close();
    	}
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public void setFileName(String logFileName) {
        this.logFileName = logFileName;
    }
}
