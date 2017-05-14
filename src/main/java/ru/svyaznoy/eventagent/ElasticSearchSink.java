package ru.svyaznoy.eventagent;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import java.util.Map;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

//Отправляет сообщение в elasticsearch, требует доработки для увеличения скорочти отправки сообщений

public class ElasticSearchSink extends AbstractSink implements Configurable {
    private static final Logger log = LogManager.getLogger(RabbitMQSink.class);
    private String host;
    private Integer port;
    private Integer batchSize;
    private String indexName;
    private String indexType;
    private Client client;
    private BulkRequestBuilder bulkRequestBuilder;
    private CounterGroup сounterGroup;

    @Override
    public void configure(Context context) {
        PropertyConfigurator.configure(context.getString("logproperties"));

        host = context.getString("host");
        port = context.getInteger("port");
        batchSize = context.getInteger("batchsize");
        indexName = context.getString("indexname");
        indexType = context.getString("indextype");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Transaction transaction = getChannel().getTransaction();
        transaction.begin();
        try {
            createConnection();
            try {
                int i;
                for (i = 0; i < batchSize; i++) {
                    Event event = getChannel().take();
                    if(event != null){
                        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, indexType, getID(event.getHeaders())).setSource(createMessage(event.getHeaders()));
                        bulkRequestBuilder.add(indexRequestBuilder);
                    } else {
                        break;
                    }
                }
                BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    throw new EventDeliveryException(bulkResponse.buildFailureMessage());
                } else {
                    transaction.commit();
                    closeConnection();
                    log.info(this.getName() + " published messages: " + i);
                }
            } catch (Exception ex) {
                log.error(this.getName() + " - Exception while publish: " + ex.toString());
                closeConnection();
                throw ex;
            }
            return Status.READY;
        } catch (Exception ex) {
            try {
                transaction.rollback();
            }catch (Exception e) {
            }
            return Status.BACKOFF;
        }
        finally {
            transaction.close();
        }
    }

    private XContentBuilder createMessage(Map<String, String> fields) {
        XContentBuilder builder;
        try {
            builder = jsonBuilder().startObject();
            for (String key : fields.keySet()) {
                builder.field(key, fields.get(key));
            }
            builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return builder;
    }

    private String getID(Map<String, String> fields) {
        return fields.get("transactionNumber") + fields.get("transactionTimeStamp") + fields.get("event");
    }

    private void createConnection() {
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(host, port));
        bulkRequestBuilder = client.prepareBulk();
    }

    private void closeConnection() {
        try {
            client.close();
        }catch (Exception e) {
        }
    }
}
