package ru.svyaznoy.eventagent.sql1c;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.flume.Context;

public class Sql1CSourceConfiguration {

    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private int batchSize;
    private int tableScanDelayMilliseconds;
    private int tooFewDataDelayMilliseconds;
    private String tableName;
    private String tableFieldId;
    private String tableFieldCreatedAt;
    private String tableFieldVersion;
    private String tableFieldEventType;
    private String tableFieldSerializedContent;
    private String tableFieldUserName;
    private String tableFieldMetaType;
    private String tableFieldMetaName;
    private String tableFieldRegisterWriteMode;
    private String tableFieldTaskBusinessProcess;
    private String tableFieldTaskRoutePoint;
    private String tableFieldSentAt;
	private String systemName;

    private String selectEventsQuery;
    private String updateQuery;
    
    public Sql1CSourceConfiguration(Context context) {
        host = context.getString("host", "localhost");
        port = context.getInteger("port", 1433);
        database = context.getString("database");
        user = context.getString("user");
        password = context.getString("password");
        tableScanDelayMilliseconds = context.getInteger("scan-table-delay", 500);
        tooFewDataDelayMilliseconds = context.getInteger("too-few-data-delay", 10000);
        tableName = context.getString("table-name");
        tableFieldId = context.getString("table-field-id", "Id");
        tableFieldCreatedAt = context.getString("table-field-createdat", "CreatedAt");
        tableFieldSentAt = context.getString("table-field-receiptdate", "SentAt");
        tableFieldVersion = context.getString("table-field-version", "dataVersionKey");
        tableFieldEventType = context.getString("table-field-eventtype", "EventType");
        tableFieldSerializedContent = context.getString("table-field-eventbody", "pu");
        tableFieldUserName = context.getString("table-field-username", "dbUserName");
        tableFieldMetaType = context.getString("table-field-metatype", "dataMetaType");
        tableFieldMetaName = context.getString("table-field-metaname", "dataMetaName");
        tableFieldRegisterWriteMode = context.getString("table-field-registerwritemode", "RegisterWriteMode");
        tableFieldTaskBusinessProcess = context.getString("table-field-taskbusinessprocess", "TaskBusinessProcess");
        tableFieldTaskRoutePoint = context.getString("table-field-taskroutepoint", "TaskRoutePoint");
        batchSize = context.getInteger("batch-size");
		systemName = context.getString("systemname");

        selectEventsQuery = createSelectEventsQuery();
        updateQuery = createUpdateQuery();
	}

    public static Timestamp getTimestamp() {
        return new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
    }

	public String getSystemName() {
		return systemName;
	}
    
    public String getDriverClassName()
    {
    	return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }
    
	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	public String getDatabase() {
		return database;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public String getTableName() {
		return tableName;
	}

	public String getTableFieldId() {
		return tableFieldId;
	}

	public String getTableFieldCreatedAt() {
		return tableFieldCreatedAt;
	}

	public String getTableFieldVersion() {
		return tableFieldVersion;
	}

	public String getTableFieldEventType() {
		return tableFieldEventType;
	}

	public String getTableFieldSerializedContent() {
		return tableFieldSerializedContent;
	}

	public String getTableFieldUserName() {
		return tableFieldUserName;
	}

	public String getTableFieldMetaType() {
		return tableFieldMetaType;
	}

	public String getTableFieldMetaName() {
		return tableFieldMetaName;
	}

	public String getTableFieldRegisterWriteMode() {
		return tableFieldRegisterWriteMode;
	}

	public String getTableFieldTaskBusinessProcess() {
		return tableFieldTaskBusinessProcess;
	}

	public String getTableFieldTaskRoutePoint() {
		return tableFieldTaskRoutePoint;
	}

	public String getTableFieldSentAt() {
		return tableFieldSentAt;
	}

	public String getSelectEventsQuery() {
		return selectEventsQuery;
	}

	public String getUpdateQuery() {
		return updateQuery;
	}
	
	public int getTableScanDelayMilliseconds() {
		return tableScanDelayMilliseconds;
	}

	public int getTooFewDataDelayMilliseconds() {
		return tooFewDataDelayMilliseconds;
	}

	private String createSelectEventsQuery() {
		return new StringBuilder()
				.append("SELECT TOP(").append(batchSize).append(")\n")
				.append("CAST(CAST(").append(tableFieldId).append(" AS UNIQUEIDENTIFIER) AS NCHAR(36)) AS ").append(tableFieldId).append(",\n")
				.append(tableFieldId).append(" AS IDBinary,\n")
				.append(tableFieldCreatedAt).append(",\n")
				.append(tableFieldVersion).append(",\n")
				.append(tableFieldEventType).append(",\n")
				.append(tableFieldSerializedContent).append(",\n")
				.append(tableFieldUserName).append(",\n")
				.append(tableFieldMetaType).append(",\n")
				.append(tableFieldMetaName).append(",\n")
				.append("CASE WHEN ").append(tableFieldRegisterWriteMode).append(" = 0x01 THEN 'true' ELSE 'false' END AS ").append(tableFieldRegisterWriteMode).append(",\n")
				.append(tableFieldTaskBusinessProcess).append(",\n")
				.append(tableFieldTaskRoutePoint).append("\n")
				.append("FROM   ").append(tableName).append(" WITH (READPAST)\n")
				.append("WHERE  ").append(tableFieldSentAt).append(" = '17530101' \n")
				.append("ORDER BY ").append(tableFieldId)
				.toString();
	}

	private String createUpdateQuery() {
	    return new StringBuilder()
				.append("UPDATE ").append(tableName).append(" WITH (ROWLOCK)\n")
				.append("SET ").append(tableFieldSentAt).append(" = ?\n")
				.append("WHERE ").append(tableFieldId).append(" = ?")
				.toString();
	}
}
