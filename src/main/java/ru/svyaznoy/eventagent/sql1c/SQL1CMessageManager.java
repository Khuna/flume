package ru.svyaznoy.eventagent.sql1c;


import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import ru.svyaznoy.eventagent.utils.Constants;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.apache.commons.codec.binary.Base64.decodeInteger;

public class SQL1CMessageManager {
    private Sql1CSourceConfiguration configuration;
    private List<byte[]> listID = new ArrayList<byte[]>();


    public SQL1CMessageManager(Sql1CSourceConfiguration configuration) {
        this.configuration = configuration;
    }

    public Event readResultSet(ResultSet resultSet) throws SQLException {
        Map<String, String> headers = new HashMap<String, String>();

        headers.put(Constants.EVENT, "business-event");
        headers.put(Constants.FLUME_VERSION, "1");
        headers.put(Constants.ID, resultSet.getNString(configuration.getTableFieldId()));
        headers.put(Constants.CREATED, Constants.dateFormat.format(resultSet.getTimestamp(configuration.getTableFieldCreatedAt())));
        headers.put(Constants.VERSION, decodeInteger(resultSet.getNString(configuration.getTableFieldVersion()).getBytes()).toString());
        headers.put(Constants.EVENT_TYPE, resultSet.getNString(configuration.getTableFieldEventType()));
        headers.put(Constants.BODY, resultSet.getNString(configuration.getTableFieldSerializedContent()));
        headers.put(Constants.USERNAME, resultSet.getNString(configuration.getTableFieldUserName()));
        headers.put(Constants.METATYPE, resultSet.getNString(configuration.getTableFieldMetaType()));
        headers.put(Constants.METANAME, resultSet.getNString(configuration.getTableFieldMetaName()));
        headers.put(Constants.REGISTER_WRITER_MODE, resultSet.getString(configuration.getTableFieldRegisterWriteMode()));
        headers.put(Constants.TASK_BUSINESS_PROCESS, resultSet.getNString(configuration.getTableFieldTaskBusinessProcess()));
        headers.put(Constants.TASK_ROUTE_POINT, resultSet.getNString(configuration.getTableFieldTaskRoutePoint()));
        headers.put(Constants.SYSTEMNAME, configuration.getSystemName());

        listID.add(resultSet.getBytes("IDBinary"));
        String body = resultSet.getNString(configuration.getTableFieldSerializedContent());

        return EventBuilder.withBody(body.getBytes(Constants.TEXT_CHARSET), headers);
    }

    public List<byte[]> getListID() {
        return listID;
    }

    public void newStart() {
        listID = new ArrayList<byte[]>();
    }
}
