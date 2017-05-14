package ru.svyaznoy.eventagent.utils;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;


public class Constants {
	public static final String TEXT_CHARSET_NAME = "UTF-8";
	public static final String GZIP_CHARSET_NAME = "gzip";
	public static final Charset TEXT_CHARSET = Charset.forName(TEXT_CHARSET_NAME);

	public static final String BODY = "body";
	public static final String FLUME_VERSION = "event-serialization-version";
	public static final String EVENT = "business-event";
	public static final String EVENT_TYPE = "business-event-type";
	public static final String ID = "business-event-id";
	public static final String VERSION = "business-event-version";
	public static final String CREATED = "business-event-createdat";
	public static final String USERNAME = "business-event-username";
	public static final String METATYPE = "business-event-metatype";
	public static final String METANAME = "business-event-metaname";
	public static final String REGISTER_WRITER_MODE = "business-event-registerwritemode";
	public static final String TASK_BUSINESS_PROCESS = "business-event-taskbusinessprocess";
	public static final String TASK_ROUTE_POINT = "business-event-taskroutepoint";
	public static final String START_SOURCE = "startSource";
	public static final String START_SINK = "startSink";
	public static final String SYSTEMNAME = "systemName";

	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	public static final SimpleDateFormat dateFormatTechLog = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
}
