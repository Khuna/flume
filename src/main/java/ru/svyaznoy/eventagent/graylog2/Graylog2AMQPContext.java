package ru.svyaznoy.eventagent.graylog2;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.Context;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Graylog2AMQPContext {

    public final static String CONFIG_SYSTEMNAME="systemname";
    public final static String CONFIG_HOSTNAME="hostname";
    public final static String CONFIG_PORT="port";
    public final static String CONFIG_USERNAME="username";
    public final static String CONFIG_PASSWORD="password";
    public final static String BATCH_SIZE ="batch-size";
    public final static String CONFIG_EXCHANGENAME="exchangename";
    public final static String CONFIG_VIRTUALHOST="virtualhost";
    public final static String SOAP_ACTION = "SOAP_ACTION";
    public final static String CONFIG_CONTENTTYPE = "contenttype";
    public final static String DEFAULT_CONFIG_CONTENTTYPE = "application/json; charset=utf-8";
    public final static String CONFIG_SOURCE="source";
    public final static String CONFIG_TOPIC="topic";
    public final static String CONFIG_CONTENT_ENCODING="contentencoding";

    private static final Logger log = LogManager.getLogger(Graylog2AMQPContext.class);

    private ConnectionFactory connectionFactory;
    private String exchangeName;
    private String contentType;
    private Integer batchSize;
    private String source;
    private String topic;
    private String contentEncoding;


    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public Graylog2AMQPContext(Context context) {
        connectionFactory = getFactory(context);
        exchangeName = context.getString(CONFIG_EXCHANGENAME, "");
        contentType = context.getString(CONFIG_CONTENTTYPE, DEFAULT_CONFIG_CONTENTTYPE);
        batchSize = context.getInteger(BATCH_SIZE);
        source = context.getString(CONFIG_SOURCE, "default");
        topic = context.getString(CONFIG_TOPIC);
        contentEncoding = context.getString(CONFIG_TOPIC);
        PropertyConfigurator.configure(context.getString("logproperties"));
    }

    private ConnectionFactory getFactory(Context context){
        ConnectionFactory factory = new ConnectionFactory();

        String hostname = context.getString(CONFIG_HOSTNAME);
        factory.setHost(hostname);

        String virtualHost = context.getString(CONFIG_VIRTUALHOST, "/");
        factory.setVirtualHost(virtualHost);

        int port = context.getInteger(CONFIG_PORT);
        factory.setPort(port);

        String username = context.getString(CONFIG_USERNAME);
        factory.setUsername(username);

        String password = context.getString(CONFIG_PASSWORD);
        factory.setPassword(password);

        return factory;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public String getContentType() {
        return contentType;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public String getSource() {
        return source;
    }

    public String getTopic() {
        return topic;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }
}
