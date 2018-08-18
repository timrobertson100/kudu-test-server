package org.apache.kudu.demo;

import org.apache.kudu.embedded.EmbeddedKudu;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.MiniKuduCluster;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;

/**
 * A quick demo showing usage.
 * <p>In reality this would be a JUnit test and the documentation would describe it as such. This is just to demonstrate the concept.
 */
public class EmbeddedKuduDemo {
  static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64).key(true).build(),
              new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING)
                  .nullable(false)
                  .desiredBlockSize(4096)
                  .encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                  .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.NO_COMPRESSION)
                  .build()));


  public static void main(String[] args) throws Exception {
    System.out.println("Prepare the binaries for embedded use");
    EmbeddedKudu.prepare();

    // What follows is vanilla usage of minicluster. The above line is the important bit.

    System.out.println("Creating cluster");
    MiniKuduCluster miniCluster =
        new MiniKuduCluster.MiniKuduClusterBuilder().numMasters(1).numTservers(1).build();

    System.out.println("Creating client");
    KuduClient client = new KuduClient.KuduClientBuilder(miniCluster.getMasterAddresses()).build();

    System.out.println("Creating table");
    KuduTable table = client.createTable(
        "test",
        SCHEMA,
        new CreateTableOptions()
            .setRangePartitionColumns(ImmutableList.of("id"))
            .setNumReplicas(1));

    System.out.println("Inserting");
    Upsert upsert = table.newUpsert();
    PartialRow row = upsert.getRow();
    row.addLong("id", 1);
    row.addString("name", "Kudu");
    KuduSession session = client.newSession();
    session.apply(upsert);
    session.close();

    System.out.println("Counting");
    KuduScanner scanner = client.newScannerBuilder(table).build();
    try {
      int rowCount = 0;
      while (scanner.hasMoreRows()) {
        rowCount += scanner.nextRows().getNumRows();
      }
      System.out.println("Rows " + rowCount);
    } finally {
      scanner.close();
    }
  }}
