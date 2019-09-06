package com.cb.demo;

import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionJsonDocument;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;


/**
 * How to run this demo:
 * 1) Change the CB's connection/username/password
 * 2) right click on this class and in the context menu click "Run"
 */
public class TxDemo {

    private static final String BUCKET = "test";
    private static final String CLUSTER_ADDRESS = "localhost";
    private static final String USERNAME = "test";
    private static final String PASSWORD = "couchbase";

    private static String customerId = "andy";
    private static String customerEvtId = "andyEvents";
    private static final Logger logger = LoggerFactory.getLogger(TxDemo.class);

    public static void main(String[] args) {

        //1) Connect to the cluster
        Cluster cluster = Cluster.connect(CLUSTER_ADDRESS, USERNAME, PASSWORD);
        Bucket bucket = cluster.bucket("test");
        Collection col = bucket.defaultCollection();

        //2) Populate the database
        createCustomer(bucket);

        // 3) Create the transaction config :
        // Durability: NONE/MAJORITY/PERSIST_TO_MAJORITY/MAJORITY_AND_PERSIST_ON_MASTER
        // TIMEOUT: Max TTL of the transaction
        // OBS: As I'm running in a single node the Durability is set to None
        Transactions transactions = Transactions.create(cluster, TransactionConfigBuilder.create()
                .durabilityLevel(TransactionDurabilityLevel.NONE)
                .build());

        try {


            transactions.run((ctx) -> {

                logger.info("Starting transaction, player {} is hitting monster {} for {} points of damage",
                        "a", "b", "c");
                //Getting all documents involved in the transactions
                //There is no virtual limit on number of documents per transaction
                TransactionJsonDocument userTx = ctx.getOrError(col, customerId);
                TransactionJsonDocument userEventsTx = ctx.getOrError(col, customerEvtId);

                JsonObject user = userTx.contentAsObject();
                JsonObject userEvents = userEventsTx.contentAsObject();


                //updating documents
                String message = "Hello there! Your trial will expire in 3 days.";
                sendEmail(message);
                user.put("followups", user.getInt("followups")+1);
                userEvents.getArray("events").add(
                    JsonObject.create()
                            .put("type", "EMAIL")
                            .put("evtDate", new Date().getTime())
                            .put("message", message)
                );

                //replace both documents
                ctx.replace(userTx, user);
                ctx.replace(userEventsTx, userEvents);

                //uncomment this line to force a rollback
                //throw new IllegalStateException("Emulating a rollback");

                //optional
                //ctx.commit();
            });
        } catch (TransactionFailed e) {
            e.printStackTrace();
            for (LogDefer err : e.result().log().logs()) {
                System.err.println(err.toString());
            }
        }

    }


    private static void sendEmail(String message) {
        logger.info("HTTP Request: Sending an email to the client...");
    }


    private static void createCustomer(Bucket bucket) {


        try {
            bucket.defaultCollection().exists(customerId);
            logger.info("User already exists...");
        } catch(KeyNotFoundException e) {
            logger.info("Creating user ...");
            JsonObject customer = JsonObject.create()
                    .put("type", "customer")
                    .put("name", "Andy")
                    .put("followups", 0);
            bucket.defaultCollection().upsert(customerId, customer);
        }


        try {
            bucket.defaultCollection().exists(customerEvtId);
            logger.info("User events exists...");
        } catch(KeyNotFoundException e) {
            logger.info("Creating user events...");
            JsonObject customerEvents = JsonObject.create()
                    .put("type", "events")
                    .put("customerId", "customerId")
                    .put( "events",  JsonArray.empty());
            bucket.defaultCollection().upsert(customerEvtId, customerEvents);
        }
    }

    private static void deleteAll(Cluster cluster) {
        cluster.query("delete from "+BUCKET);
    }
}
