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
 * ###### Import the data with:
 * cbimport  json -c couchbase://127.0.0.1 -u Administrator -p password -b test -d file:///Users/deniswsrosa/Downloads/crm_list.json -f list -g %id%  -t 4
 *
 * ###### How to run this demo:
 * 1) Change the CB's connection/username/password
 * 2) right click on this class and in the context menu click "Run"
 *
 * ###### Use Case:
 *
 *  In this use case we want to keep track of all interactions with an account in order to build a customer360 system,
 *  Whenever an email is sent to a client we will execute the following:
 *
 *  1) Increase the total of interactions whith the target account into the "account"
 *  2) Update the lastInteraction
 *  2) Add an item to the accountEvent document with the type of the event and its content.
 *
 */
public class TxDemo {

    private static final String BUCKET = "test";
    private static final String CLUSTER_ADDRESS = "localhost";
    private static final String USERNAME = "test";
    private static final String PASSWORD = "couchbase";

    private static String accountId = "acc1";
    private static String accountEvtId = "acc1evt";
    private static final Logger logger = LoggerFactory.getLogger(TxDemo.class);

    public static void main(String[] args) {

        //1) Connect to the cluster
        Cluster cluster = Cluster.connect(CLUSTER_ADDRESS, USERNAME, PASSWORD);
        Bucket bucket = cluster.bucket(BUCKET);
        Collection col = bucket.defaultCollection();

        //2) Create Event for the account
        createAccountEvent(bucket);

        // 3) Create the transaction config :
        // Durability: NONE/MAJORITY/PERSIST_TO_MAJORITY/MAJORITY_AND_PERSIST_ON_MASTER
        // TIMEOUT: Max TTL of the transaction
        // OBS: As I'm running in a single node the Durability is set to None
        Transactions transactions = Transactions.create(cluster, TransactionConfigBuilder.create()
                .durabilityLevel(TransactionDurabilityLevel.NONE)
                .build());

        try {


            transactions.run((ctx) -> {

                logger.info("Starting transaction for account {} and accountEvt {}", accountId, accountEvtId);
                //Getting all documents involved in the transactions
                //There is no virtual limit on number of documents per transaction
                TransactionJsonDocument accountTx = ctx.getOrError(col, accountId);
                TransactionJsonDocument accountEventsTx = ctx.getOrError(col, accountEvtId);

                JsonObject account = accountTx.contentAsObject();
                JsonObject accountEvents = accountEventsTx.contentAsObject();

                //updating documents
                String message = "Hey! What's up?";
                sendEmail(message);
                account.put("followups", account.getInt("followups") == null? 1: account.getInt("followups")+1);
                account.put("lastInteraction", new Date().getTime());
                accountEvents.getArray("events").add(
                    JsonObject.create()
                            .put("type", "EMAIL")
                            .put("evtDate", new Date().getTime())
                            .put("message", message)
                );

                //replace both documents
                ctx.replace(accountTx, account);
                ctx.replace(accountEventsTx, accountEvents);

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


    private static void createAccountEvent(Bucket bucket) {

        try {
            bucket.defaultCollection().exists(accountEvtId);
            logger.info("Account events already exists...");
        } catch(KeyNotFoundException e) {
            logger.info("Creating account events...");
            JsonObject customerEvents = JsonObject.create()
                    .put("type", "accountEvents")
                    .put("accountId", accountId)
                    .put( "events",  JsonArray.empty());
            bucket.defaultCollection().upsert(accountEvtId, customerEvents);
        }
    }

}
