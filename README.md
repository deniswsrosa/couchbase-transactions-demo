# Transactions Demo (Java)

## Goal

The goal of this demo is to show the basics of client-side transactions available in the Couchbase Java SDK 3.x and Couchbase Server 6.5 Beta. It will show a basic transaction that affects two documents. It can also show a rollback.

There are two documents: acc1 and acc1evt.

In this transaction, a new followup event will be added to acc1evt. acc1 will also be updated to increase the number of follows as well as the date of the last followup.

## Initial setup

0. Import CRM data with a command like the following:

`PS C:\projects\couchbase-transactions-demo> & 'C:\Program Files\Couchbase\Server\bin\cbimport.exe' json -c couchbase://localhost -u <username> -p <password> -b <bucket name> -d file://crm_list.json -f list -g %id% -t 4`

If you see error messages about 183 documents not being imported, it is safe to ignore (for the purposes of this demo).

1. Modify the constants in `TxDemo.java`:
   1. BUCKET - name of the bucket you've imported CRM data into
   2. CLUSTER_ADDRESS - address of a node in a Couchbase cluster
   3. USERNAME/PASSWORD - Couchbase cluster credentials

## Demo steps

0. Observe acc1 and acc1evt documents in the bucket

1. Open project in VSCode or IntelliJ or whatever

2. Right click on TxDemo class and select "Run"

3. Observe the program. Look at acc1 and acc1evt. The `lastInteraction` field in acc1 should match the `evtDate` in the acc1evt document in the events array.

4. Change the `message` and run the program again. Repeat step 5.

5. Change the `message` again, and uncomment `IllegalStateException` to cause a rollback. Run the program again.

6. Observe that acc1 and acc1evt are in a consistent state.
