package ru.svyaznoy.eventagent;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ru.svyaznoy.eventagent.logtech1c.LogTech1CManager;
import ru.svyaznoy.eventagent.logtech1c.LogTech1CPropertiesManager;

import java.io.IOException;
import java.util.List;

public class LogTech1CSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger log = LogManager.getLogger(LogTech1CSource.class);
    private LogTech1CManager logManager;


    @Override
    public void configure(Context context) {
        PropertyConfigurator.configure(context.getString("logproperties"));
        try {
            logManager = new LogTech1CManager(context);
        } catch (IOException e) {
            log.error("cant create context");
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        List<Event> events;
        try {
            events = logManager.getEvents(logManager.getBatchSize());
            if (events.size() > 0) {
                getChannelProcessor().processEventBatch(events);
            }
            logManager.savePropsFiles();
            return Status.READY;
        } catch (IOException e) {
            return Status.BACKOFF;
        }
    }
}
