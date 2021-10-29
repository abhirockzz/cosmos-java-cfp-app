package com.abhirockzz;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;

public class App {
    static String cosmosDBEndpoint;
    static String cosmosDBaccessKey;
    static String cosmosDBDatabaseName;

    static String cosmosDBMonitoredContainer;
    static String cosmosDBLeaseContainer;
    static String outputContainerName; // pre-req: create this manually

    static String hostName;

    static String COSMOSDB_ENDPOINT_ENV = "COSMOSDB_ENDPOINT";
    static String COSMOSDB_ACCESS_KEY_ENV = "COSMOSDB_ACCESS_KEY";
    static String COSMOSDB_DATABASE_NAME_ENV = "COSMOSDB_DATABASE_NAME";
    static String COSMOSDB_MONITORED_CONTAINER_NAME_ENV = "COSMOSDB_MONITORED_CONTAINER_NAME";
    static String COSMOSDB_LEASE_CONTAINER_NAME_ENV = "COSMOSDB_LEASE_CONTAINER_NAME";
    static String COSMOSDB_OUTPUT_CONTAINER_NAME_ENV = "COSMOSDB_OUTPUT_CONTAINER_NAME";

    static String HOST_NAME_ENV = "HOSTNAME"; // in k8s, this will use pod name automatically. if running outside of
                                              // k8s, it needs to be set (or random UUID will be used)

    static String leasePrefix = "java-cfp-app"; // TODO: expose as env var

    private static void setup() {
        Map<String, String> envVars = System.getenv();
        validateEnvVars(envVars);

        cosmosDBEndpoint = envVars.get(COSMOSDB_ENDPOINT_ENV);
        cosmosDBaccessKey = envVars.get(COSMOSDB_ACCESS_KEY_ENV);
        cosmosDBDatabaseName = envVars.get(COSMOSDB_DATABASE_NAME_ENV);
        cosmosDBMonitoredContainer = envVars.get(COSMOSDB_MONITORED_CONTAINER_NAME_ENV);
        cosmosDBLeaseContainer = envVars.get(COSMOSDB_LEASE_CONTAINER_NAME_ENV);
        outputContainerName = envVars.get(COSMOSDB_OUTPUT_CONTAINER_NAME_ENV);

        hostName = envVars.get(HOST_NAME_ENV);
        if (hostName == null) {
            System.out.println("missing HOST_NAME env var. generating random UUID");
            hostName = UUID.randomUUID().toString();
        }
        System.out.println("host: " + hostName);
    }

    private static void validateEnvVars(Map<String, String> envVars) {
        if (!envVars.containsKey(COSMOSDB_ENDPOINT_ENV)) {
            throw new RuntimeException("Missing environment variable " + COSMOSDB_ENDPOINT_ENV);
        }
        if (!envVars.containsKey(COSMOSDB_ACCESS_KEY_ENV)) {
            throw new RuntimeException("Missing environment variable " + COSMOSDB_ACCESS_KEY_ENV);
        }
        if (!envVars.containsKey(COSMOSDB_DATABASE_NAME_ENV)) {
            throw new RuntimeException("Missing environment variable " + COSMOSDB_DATABASE_NAME_ENV);
        }
        if (!envVars.containsKey(COSMOSDB_MONITORED_CONTAINER_NAME_ENV)) {
            throw new RuntimeException("Missing environment variable " + COSMOSDB_MONITORED_CONTAINER_NAME_ENV);
        }
        if (!envVars.containsKey(COSMOSDB_LEASE_CONTAINER_NAME_ENV)) {
            throw new RuntimeException("Missing environment variable " + COSMOSDB_LEASE_CONTAINER_NAME_ENV);
        }
        if (!envVars.containsKey(COSMOSDB_OUTPUT_CONTAINER_NAME_ENV)) {
            throw new RuntimeException("Missing environment variable " + COSMOSDB_OUTPUT_CONTAINER_NAME_ENV);
        }
        if (!envVars.containsKey(HOST_NAME_ENV)) {
            hostName = UUID.randomUUID().toString();
        }
    }

    static CosmosAsyncDatabase database;

    public static void main(String[] args) throws Exception {
        setup();

        CosmosAsyncClient client = new CosmosClientBuilder().endpoint(cosmosDBEndpoint).key(cosmosDBaccessKey)
                .contentResponseOnWriteEnabled(true).buildAsyncClient();
        database = client.getDatabase(cosmosDBDatabaseName);

        // createContainer(database, outputContainerName);

        ChangeFeedProcessorOptions cfOptions = new ChangeFeedProcessorOptions();
        cfOptions.setFeedPollDelay(Duration.ofMillis(100)).setStartFromBeginning(true).setLeasePrefix(leasePrefix);
        // .setLeaseAcquireInterval(Duration.ofMillis(1000));

        ChangeFeedProcessor cfp = new ChangeFeedProcessorBuilder().hostName(hostName)
                .leaseContainer(createContainer(database, cosmosDBLeaseContainer))
                // .leaseContainer(database.getContainer("lease"))
                .feedContainer(database.getContainer(cosmosDBMonitoredContainer)).handleChanges(new CFPProcessor())
                .options(cfOptions).buildChangeFeedProcessor();

        cfp.start().block();
        System.out.println("ChangeFeedProcessor started? " + cfp.isStarted());

    }

    static class CFPProcessor implements Consumer<List<JsonNode>> {
        public CFPProcessor() {
        }

        @Override
        public void accept(List<JsonNode> feed) {
            for (JsonNode doc : feed) {
                System.out.println("change feed doc: " + doc.toPrettyString());
                CosmosAsyncContainer outputContainer = database.getContainer(outputContainerName);
                try {
                    // outputContainer.delete();
                    outputContainer.createItem(doc).block();
                    System.out.println("saved doc to container " + outputContainerName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static CosmosAsyncContainer createContainer(CosmosAsyncDatabase database, String name) {
        System.out.println("creating container " + name);
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        database.createContainerIfNotExists(name, "/id", throughputProperties).block();
        return database.getContainer(name);
    }
}
