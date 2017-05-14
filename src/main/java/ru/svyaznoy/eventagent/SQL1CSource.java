package ru.svyaznoy.eventagent;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import ru.svyaznoy.eventagent.sql1c.SQL1CMessageManager;
import ru.svyaznoy.eventagent.sql1c.Sql1CSourceConfiguration;
import ru.svyaznoy.eventagent.utils.Utils;

public class SQL1CSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger log = LogManager.getLogger(SQL1CSource.class);
    
    private BasicDataSource dataSource;
    private Sql1CSourceConfiguration configuration;
    private SQL1CMessageManager messageManager;

    @Override
    public void configure(Context context) {
        PropertyConfigurator.configure(context.getString("logproperties"));
        configuration = new Sql1CSourceConfiguration(context);
        messageManager = new SQL1CMessageManager(configuration);
        configureDataSource();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Connection connection = null;
        messageManager.newStart();
        try {
            connection = createConnection();
            List<Event> eventList = getEvents(connection);
            log.debug(String.format("%d events have been get", eventList.size()));
            if (eventList.size() != 0) {
                sendEvents(getChannelProcessor(), eventList);
                log.info(String.format("%d events have been sent", eventList.size()));
                markEvents(connection, messageManager.getListID());
                log.info(String.format("%d events have been marked", eventList.size()));
            }
            // задержимся, если получили данных меньше, чем размер пачки
            // это говорит о том, что мы читаем быстрее, чем они поступают в таблицу
            if (eventList.size() < configuration.getBatchSize() &&
            		configuration.getTooFewDataDelayMilliseconds() > 0) {
            	log.debug(String.format("Waiting for %dms after processing %d events", 
            			configuration.getTooFewDataDelayMilliseconds(), eventList.size()));
            	
            	silentSleep(configuration.getTooFewDataDelayMilliseconds());
            }
        }
        catch (ClassNotFoundException | SQLException ex) {
        	log.error("Failed to grab events from SQL Server", ex);
        	
            status = Status.BACKOFF;
        }
        catch (ChannelException | FlumeException ex) {
        	log.error("Failed to push events to flume", ex);
        	
        	throw new EventDeliveryException(ex);
        }
        finally {
        	closeConnection(connection);
        	connection = null;
        }
        // задержка на каждый цикл опроса
        if (configuration.getTableScanDelayMilliseconds() > 0) {
        	silentSleep(configuration.getTableScanDelayMilliseconds());
        }
        
        return status;
    }

    private void silentSleep(int delayMilliseconds) {
		try {
			Thread.sleep(delayMilliseconds);
		}
		catch (InterruptedException ex) {
		}
	}
    
    private void configureDataSource() {
    	dataSource = new BasicDataSource();
    	dataSource.setDriverClassName(configuration.getDriverClassName());
    	
        String host = configuration.getHost();
        String port = configuration.getPort().toString();
        String database = configuration.getDatabase();
        String user = configuration.getUser();
        String password = configuration.getPassword();
    	String connectionUrl = String.format(
                "jdbc:sqlserver://%s:%s;databaseName=%s;user=%s;password=%s",
                host, port, database, user, password);
    	
    	dataSource.setUrl(connectionUrl);
    	dataSource.setDefaultAutoCommit(false);
    }

	public Connection createConnection() throws ClassNotFoundException, SQLException {
        return dataSource.getConnection();
    }

    public List<Event> getEvents(Connection connection) throws SQLException {
        Statement statement = null;
        List<Event> events = new ArrayList<Event>();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(configuration.getSelectEventsQuery());
        while (resultSet.next()) {
            Event event = messageManager.readResultSet(resultSet);
            events.add(event);
        }
        return events;
    }

    public void sendEvents(ChannelProcessor channelProcessor, List<Event> eventList) {
        channelProcessor.processEventBatch(eventList);
    }

    public void markEvents(Connection connection,  List<byte[]> listID) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(configuration.getUpdateQuery());
        Timestamp timestamp = Utils.getTimestamp();
        for (byte[] ID : listID) {
            statement.setTimestamp(1, timestamp);
            statement.setBytes(2, ID);
            statement.executeUpdate();
            connection.commit();
        }
    }

	private void closeConnection(Connection connection) {
		try {
	    	if (connection != null)
	    		connection.close();
		}
		catch (SQLException ex) {
			log.warn("Unable to close connection", ex);
		}
	}
}
