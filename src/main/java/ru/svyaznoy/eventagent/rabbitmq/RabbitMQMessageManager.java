package ru.svyaznoy.eventagent.rabbitmq;

import org.apache.flume.Event;
import ru.svyaznoy.eventagent.utils.Constants;
import ru.svyaznoy.eventagent.utils.Translit;
import ru.svyaznoy.eventagent.utils.Utils;

public class RabbitMQMessageManager {
    public String format(Event event) {
        String systemName = event.getHeaders().get(Constants.SYSTEMNAME);
        String createdAt = event.getHeaders().get(Constants.CREATED);
        String userName = event.getHeaders().get(Constants.USERNAME);
        String metaType = event.getHeaders().get(Constants.METATYPE);
        String metaName = event.getHeaders().get(Constants.METANAME);
        String id = event.getHeaders().get(Constants.ID);
        String body = event.getHeaders().get(Constants.BODY);
        String version = event.getHeaders().get(Constants.VERSION);
        String actiontype = event.getHeaders().get(Constants.EVENT_TYPE);
        String registerWriteMode = event.getHeaders().get(Constants.REGISTER_WRITER_MODE);
        String taskBusinessProcess = event.getHeaders().get(Constants.TASK_BUSINESS_PROCESS);
        String taskRoutePoint = event.getHeaders().get(Constants.TASK_ROUTE_POINT);

        String atom = new StringBuilder()
                .append("<feed xmlns=\"http://www.w3.org/2005/Atom\">\n")
                .append("\t<title>Data feed from ").append(systemName).append("</title>\n")
                .append("\t<updated>").append(createdAt).append("</updated>\n")
                .append("\t<id>").append(Utils.parseSQLIDTo1CID(id)).append("</id>\n")
                .append("\t<author>").append(userName).append("</author>\n")
                .append("\t<generator>").append(systemName).append("</generator>\n")
                .append("\t<entry>\n")
                .append("\t\t<title>").append(metaType).append(".").append(metaName).append("</title>\n")
                .append("\t\t<updated>").append(createdAt).append("</updated>\n")
                .append("\t\t<id>").append(id).append("</id>\n")
                .append("\t\t<category term=\"").append(metaType).append("\" label=\"").append(metaName).append("\" />\n")
                .append("\t\t<content>").append(body).append("</content>\n")
                .append("\t\t<version>").append(version).append("</version>\n")
                .append("\t\t<actiontype>").append(actiontype).append("</actiontype>\n")
                .append("\t\t<registerwritemode>").append(registerWriteMode).append("</registerwritemode>\n")
                .append("\t\t<taskbusinessprocess>").append(taskBusinessProcess).append("</taskbusinessprocess>\n")
                .append("\t\t<taskroutepoint>").append(taskRoutePoint).append("</taskroutepoint>\n")
                .append("\t</entry>\n")
                .append("</feed>")
                .toString();

        return atom;
    }

    public String getTopic(Event event) {
        String systemName = event.getHeaders().get(Constants.SYSTEMNAME);
        String metaType = event.getHeaders().get(Constants.METATYPE);
        String metaName = event.getHeaders().get(Constants.METANAME);

        String topicName = new StringBuilder()
                .append(systemName).append(".")
                .append(metaType).append(".")
                .append(metaName)
                .toString();

        return Translit.toTranslit(topicName);
    }
}
