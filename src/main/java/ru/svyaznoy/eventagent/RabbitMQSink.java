package ru.svyaznoy.eventagent;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ru.svyaznoy.eventagent.rabbitmq.RabbitMQConfiguration;
import ru.svyaznoy.eventagent.rabbitmq.RabbitMQMessageManager;
import ru.svyaznoy.eventagent.utils.Constants;
import java.io.IOException;
import java.util.*;

public class RabbitMQSink extends AbstractSink implements Configurable {
    private static final Logger log = LogManager.getLogger(RabbitMQSink.class);

    private org.apache.flume.Channel flumeChannel;
    private RabbitMQConfiguration configuration;
    private RabbitMQMessageManager messageManager;

    @Override
    public void configure(Context context) {
        PropertyConfigurator.configure(context.getString("logproperties"));
        configuration = new RabbitMQConfiguration(context);
    }

    @Override
    public Status process() throws EventDeliveryException {
        flumeChannel = getChannel();
        Transaction transaction = flumeChannel.getTransaction();
        transaction.begin();
        Status result = null;
        try {
            List<Event> eventList = getEvents(flumeChannel);
            if (eventList.size() != 0) {
                publishEvents(eventList);
            } else {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ex) {
                }
            }
            transaction.commit();
            result = Status.READY;
        }
        catch (IOException ex) {
            transaction.rollback();
            log.error("Failed to send events", ex);
        	result = Status.BACKOFF;
        }
        finally {
            transaction.close();
        }
        return result;
    }

	private void publishEvents(List<Event> eventList) throws IOException {
		Connection connection = null;
		try {
			log.debug(String.format("Trying to publish %d events. Exchange=%s",
                    eventList.size(), configuration.getExchangeName()));
            boolean noException = true;
            try {
                connection = createConnection();
            } catch (IOException e) {
                log.error("Cant create connection to RabbitMQ");
                log.error(e.getMessage());
                noException = false;
            }
            if (noException) {
                messageManager = new RabbitMQMessageManager();
                sendEvents(connection, eventList);
                log.info("Published " + eventList.size() + " events");
            } else throw new IOException();
		}
		finally {
			try {
		    	if (connection != null)
		    		connection.close();
			}
			catch (IOException ex) {
				log.warn("Failed to close AMQP connection", ex);
                throw new IOException();
			}
		}
	}

    public Connection createConnection() throws IOException {
        return configuration.getConnectionFactory().newConnection();
    }

    private List<Event> getEvents(org.apache.flume.Channel flumeChannel) throws ChannelException {
        List<Event> eventList = new ArrayList<Event>();
        String startTime = Constants.dateFormat.format(Calendar.getInstance().getTime());

        for (int i = 0; i < configuration.getBatchSize(); i++) {
            Event event = flumeChannel.take();
            if (event != null) {
                eventList.add(event);
            }
        }

        return eventList;
    }

    private void sendEvents(Connection connection, List<Event> eventList) throws IOException {
        Channel channel = null;
        
        try {
        	channel = connection.createChannel();
        	
            channel.exchangeDeclare(configuration.getExchangeName(), "topic", true);
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put(RabbitMQConfiguration.SOAP_ACTION, "greet");

            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .deliveryMode(2)
                    .contentType(configuration.getContentType())
                    .contentEncoding(Constants.TEXT_CHARSET_NAME)
                    .headers(headers)
                    .build();

            for (Event event : eventList) {
                String message = messageManager.format(event);
                String topic =  messageManager.getTopic(event);
                log.debug(String.format("Sending message with topic: %s", topic));
                channel.basicPublish(configuration.getExchangeName(),
                        topic, props, message.getBytes(Constants.TEXT_CHARSET));
            }        	
        }
        catch (Exception ex){
            throw new IOException();
        }
        finally {
        	try {
	        	if (channel != null)
	        		channel.close();
        	}
        	catch (IOException ex) {
        		log.warn("Failed to close channel", ex);
                throw new IOException();
        	}
        }
    }
}
