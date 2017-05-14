package ru.svyaznoy.eventagent;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ru.svyaznoy.eventagent.graylog2.Graylog2AMQPContext;
import ru.svyaznoy.eventagent.graylog2.Graylog2Message;
import ru.svyaznoy.eventagent.utils.Constants;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class Graylog2SinkAMQP extends AbstractSink implements Configurable {
    private static final Logger log = LogManager.getLogger(Graylog2SinkAMQP.class);

    private org.apache.flume.Channel flumeChannel;
    private Graylog2AMQPContext graylog2AMQPContext;

    @Override
    public void configure(Context context) {
        PropertyConfigurator.configure(context.getString("logproperties"));
        graylog2AMQPContext = new Graylog2AMQPContext(context);
    }

    @Override
    public Status process() throws EventDeliveryException {
        flumeChannel = getChannel();
        Transaction transaction = flumeChannel.getTransaction();
        Sink.Status result = null;
        try {
            transaction.begin();
            List<Graylog2Message> messages = getEvents(getChannel());
            if (messages.size() != 0) {
                publishEvents(messages);
                result = Sink.Status.READY;
                log.info("send " + messages.size() + " messages");
            }
        }
        catch (Exception ex) {
            log.error("Failed to send events", ex);
            result = Sink.Status.BACKOFF;
        }

        if (result == Sink.Status.READY) {
            transaction.commit();
        } else {
            transaction.rollback();
            result = Sink.Status.BACKOFF;
        }
        transaction.close();

        return result;
    }

    private void publishEvents(List<Graylog2Message> messages) throws Exception {
        Connection connection = null;
        try {
            log.debug(String.format("Trying to publish %d events. Exchange=%s",
                    messages.size(), graylog2AMQPContext.getExchangeName()));
            try {
                connection = createConnection();
            } catch (Exception e) {
                log.error("Cant create connection to RabbitMQ");
                log.error(e.getMessage());
            }
            sendEvents(connection, messages);
            log.info("Published " + messages.size() + " events");
        }
        finally {
            try {
                if (connection != null)
                    connection.close();
            }
            catch (IOException ex) {
                log.warn("Failed to close AMQP connection", ex);
            }
        }
    }

    public Connection createConnection() throws IOException {
        return graylog2AMQPContext.getConnectionFactory().newConnection();
    }

    private List<Graylog2Message> getEvents(org.apache.flume.Channel flumeChannel) throws ChannelException {
        List<Graylog2Message> messages = new ArrayList<Graylog2Message>();
        for (int i = 0; i < graylog2AMQPContext.getBatchSize(); i++) {
            Event event = flumeChannel.take();
            if (event != null) {
                Graylog2Message message = new Graylog2Message("_", "__", getTimestump(event), "1");
                message.setAdditonalFields(getAdditionalFields(event));
                message.setHost(graylog2AMQPContext.getSource());
                messages.add(message);
            }
        }

        return messages;
    }

    private long getTimestump(Event event) {
        try {
            return Constants.dateFormatTechLog.parse(event.getHeaders().get("date")).getTime();
        } catch (ParseException e) {
            return Calendar.getInstance().getTime().getTime();
        }
    }

    private Map<String, String> getAdditionalFields(Event event) {
        Map<String, String> additional = new HashMap<>();
        for (String key : event.getHeaders().keySet()) {
            if (key.equals("time") || key.equals("date")) {
            } else {
                additional.put(key, event.getHeaders().get(key));
            }
        }
        return additional;
    }

    private void sendEvents(Connection connection, List<Graylog2Message> messages) throws Exception {
        Channel channel = null;
        try {
            channel = connection.createChannel();
            channel.exchangeDeclare(graylog2AMQPContext.getExchangeName(), "topic", true);
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put(graylog2AMQPContext.SOAP_ACTION, "greet");
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .deliveryMode(2)
                    .contentType(graylog2AMQPContext.getContentType())
                    .contentEncoding(graylog2AMQPContext.getContentEncoding())
                    .headers(headers)
                    .build();
            for (Graylog2Message message : messages) {
                channel.basicPublish(graylog2AMQPContext.getExchangeName(), graylog2AMQPContext.getTopic(), props, message.toJson2().getBytes(Constants.TEXT_CHARSET));
            }
        }
        finally {
            try {
                if (channel != null)
                    channel.close();
            }
            catch (IOException ex) {
                log.debug("Failed to close channel", ex);
            }
        }
    }
}