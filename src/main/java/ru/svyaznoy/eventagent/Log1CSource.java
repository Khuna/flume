package ru.svyaznoy.eventagent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import ru.svyaznoy.eventagent.log1c.Log1CIndexManager;
import ru.svyaznoy.eventagent.log1c.Log1CLogManager;
import ru.svyaznoy.eventagent.log1c.Log1CPropertiesManager;

public class Log1CSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger log = LogManager.getLogger(Log1CSource.class);
    private Integer maxfieldsize;
    private Integer batchsize;
    private String propertiesFile;
    private String indexFile;
    private String ignorelist;
    private Log1CIndexManager indexManager;
    private Log1CLogManager logManager;

    @Override
    public void configure(Context context) {
        PropertyConfigurator.configure(context.getString("logproperties"));
        batchsize = context.getInteger("batch-size");
        maxfieldsize = context.getInteger("maxfieldsize", 999);
        propertiesFile = context.getString("fileposition");
        indexFile = context.getString("indexfile");
        ignorelist = context.getString("ignoreeventtype", "");
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            Log1CPropertiesManager propertiesManager = new Log1CPropertiesManager(propertiesFile);
            logManager = new Log1CLogManager(propertiesManager);
            logManager.setIgnorelist(ignorelist);
        } catch (Exception e) {
            log.error("error while creating file reader");
            return Status.BACKOFF;
        }
        List<Event> events;
        List<Map<String, String>> maps;
        try {
            maps = new ArrayList<Map<String, String>>();
            for (int i = 0; i < batchsize; i++) {
                Map<String, String> map = logManager.getEvent();
                if (map != null) {
                    maps.add(map);
                } else {
                    if (!logManager.findNextFile()) {
                        break;
                    }
                }
            }
            if (maps.size() > 0) {
                indexManager = new Log1CIndexManager(indexFile);
            } else {
                return Status.READY;
            }

            events = new ArrayList<Event>();
            for (Map<String, String> map : maps) {
                Event event = indexManager.toFlumeEvent(map, maxfieldsize, logManager);
                if (event != null) {
                    events.add(event);
                }

            }
            if (events.size() > 0 ) {
                getChannelProcessor().processEventBatch(events);
            }
            logManager.setPropertiesManager();
            log.info("send " + events.size() + " events");

        } catch (Exception e) {
            log.error("error while sending events", e);

            return Status.BACKOFF;
        }

        return Status.READY;
    }
}
