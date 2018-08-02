package org.apache.kudu.embedded;

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
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Start a minicluster using the embedded binaries and code against it.
 *
 * <p>This code is a quick hack.
 */
public class TestEmbedded {
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


  public static void main(String[] args) throws IOException, URISyntaxException {

    // Pull Kudu out of the jar and make executable
    System.out.println("Extracting Kudu");
    Path tmpDir = Files.createTempDirectory("kudu"); // TODO delete on exit
    tmpDir.resolve("bin").toFile().mkdir();
    tmpDir.resolve("lib").toFile().mkdir();
    extractKudu(tmpDir);
    tmpDir.resolve("bin").resolve("kudu").toFile().setExecutable(true);
    tmpDir.resolve("bin").resolve("kudu-master").toFile().setExecutable(true);
    tmpDir.resolve("bin").resolve("kudu-tserver").toFile().setExecutable(true);

    // set the location for minicluster to use
    System.setProperty("binDir", tmpDir.resolve("bin").toString());

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
  }

  /**
   * Extracts Kudu from the Jar into a temp location.
   * TODO: what follows is totally ridiculous(!) but I've not worked out how to list contents of a directory in a Jar...
   */
  private static void extractKudu(Path tmpDir) throws IOException {

    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/bin/kudu"), tmpDir.resolve("bin/kudu"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/bin/kudu-master"), tmpDir.resolve("bin/kudu-master"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/bin/kudu-tserver"), tmpDir.resolve("bin/kudu-tserver"));

    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libcfile.so"), tmpDir.resolve("lib/libcfile.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libcfile_proto.so"), tmpDir.resolve("lib/libcfile_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libclient_proto.so"), tmpDir.resolve("lib/libclient_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libclock.so"), tmpDir.resolve("lib/libclock.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libcodegen.so"), tmpDir.resolve("lib/libcodegen.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libcom_err.so.2"), tmpDir.resolve("lib/libcom_err.so.2"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libconsensus.so"), tmpDir.resolve("lib/libconsensus.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libconsensus_metadata_proto.so"), tmpDir.resolve("lib/libconsensus_metadata_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libconsensus_proto.so"), tmpDir.resolve("lib/libconsensus_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libcrcutil.so.0"), tmpDir.resolve("lib/libcrcutil.so.0"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libcrypt.so.1"), tmpDir.resolve("lib/libcrypt.so.1"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libcrypto.so.10"), tmpDir.resolve("lib/libcrypto.so.10"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libev.so.4"), tmpDir.resolve("lib/libev.so.4"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libfreebl3.so"), tmpDir.resolve("lib/libfreebl3.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libfs_proto.so"), tmpDir.resolve("lib/libfs_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libgflags.so.2.2"), tmpDir.resolve("lib/libgflags.so.2.2"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libglog.so.0"), tmpDir.resolve("lib/libglog.so.0"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libgmock.so"), tmpDir.resolve("lib/libgmock.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libgssapi_krb5.so.2"), tmpDir.resolve("lib/libgssapi_krb5.so.2"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libgutil.so"), tmpDir.resolve("lib/libgutil.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libhistogram_proto.so"), tmpDir.resolve("lib/libhistogram_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libhms_thrift.so"), tmpDir.resolve("lib/libhms_thrift.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libk5crypto.so.3"), tmpDir.resolve("lib/libk5crypto.so.3"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkeyutils.so.1"), tmpDir.resolve("lib/libkeyutils.so.1"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkrb5.so.3"), tmpDir.resolve("lib/libkrb5.so.3"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkrb5support.so.0"), tmpDir.resolve("lib/libkrb5support.so.0"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkrpc.so"), tmpDir.resolve("lib/libkrpc.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libksck.so"), tmpDir.resolve("lib/libksck.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkserver.so"), tmpDir.resolve("lib/libkserver.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_client.so"), tmpDir.resolve("lib/libkudu_client.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_client_test_util.so"), tmpDir.resolve("lib/libkudu_client_test_util.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_common.so"), tmpDir.resolve("lib/libkudu_common.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_common_proto.so"), tmpDir.resolve("lib/libkudu_common_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_fs.so"), tmpDir.resolve("lib/libkudu_fs.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_hms.so"), tmpDir.resolve("lib/libkudu_hms.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_test_util.so"), tmpDir.resolve("lib/libkudu_test_util.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_tools_rebalance.so"), tmpDir.resolve("lib/libkudu_tools_rebalance.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_tools_util.so"), tmpDir.resolve("lib/libkudu_tools_util.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_util.so"), tmpDir.resolve("lib/libkudu_util.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libkudu_util_compression.so"), tmpDir.resolve("lib/libkudu_util_compression.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/liblog.so"), tmpDir.resolve("lib/liblog.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/liblog_proto.so"), tmpDir.resolve("lib/liblog_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libm.so.6"), tmpDir.resolve("lib/libm.so.6"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libmaintenance_manager_proto.so"), tmpDir.resolve("lib/libmaintenance_manager_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libmaster.so"), tmpDir.resolve("lib/libmaster.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libmaster_proto.so"), tmpDir.resolve("lib/libmaster_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libmini_cluster.so"), tmpDir.resolve("lib/libmini_cluster.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libmini_hms.so"), tmpDir.resolve("lib/libmini_hms.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libmini_kdc.so"), tmpDir.resolve("lib/libmini_kdc.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libpb_util_proto.so"), tmpDir.resolve("lib/libpb_util_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libprofiler.so.0"), tmpDir.resolve("lib/libprofiler.so.0"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libprotobuf.so.14"), tmpDir.resolve("lib/libprotobuf.so.14"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libresolv.so.2"), tmpDir.resolve("lib/libresolv.so.2"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/librpc_header_proto.so"), tmpDir.resolve("lib/librpc_header_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/librpc_introspection_proto.so"), tmpDir.resolve("lib/librpc_introspection_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libsasl2.so.2"), tmpDir.resolve("lib/libsasl2.so.2"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libsecurity.so"), tmpDir.resolve("lib/libsecurity.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libselinux.so.1"), tmpDir.resolve("lib/libselinux.so.1"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libserver_base_proto.so"), tmpDir.resolve("lib/libserver_base_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libserver_process.so"), tmpDir.resolve("lib/libserver_process.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libsnappy.so.1"), tmpDir.resolve("lib/libsnappy.so.1"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libssl.so.10"), tmpDir.resolve("lib/libssl.so.10"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libstdc++.so.6"), tmpDir.resolve("lib/libstdc++.so.6"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtablet.so"), tmpDir.resolve("lib/libtablet.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtablet_copy_proto.so"), tmpDir.resolve("lib/libtablet_copy_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtablet_proto.so"), tmpDir.resolve("lib/libtablet_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtcmalloc.so.4"), tmpDir.resolve("lib/libtcmalloc.so.4"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libthrift.so.0.11.0"), tmpDir.resolve("lib/libthrift.so.0.11.0"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtinfo.so.5"), tmpDir.resolve("lib/libtinfo.so.5"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtoken_proto.so"), tmpDir.resolve("lib/libtoken_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtool_proto.so"), tmpDir.resolve("lib/libtool_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtserver.so"), tmpDir.resolve("lib/libtserver.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtserver_admin_proto.so"), tmpDir.resolve("lib/libtserver_admin_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtserver_proto.so"), tmpDir.resolve("lib/libtserver_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libtserver_service_proto.so"), tmpDir.resolve("lib/libtserver_service_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libunwind.so.8"), tmpDir.resolve("lib/libunwind.so.8"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libutil_compression_proto.so"), tmpDir.resolve("lib/libutil_compression_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libversion_info_proto.so"), tmpDir.resolve("lib/libversion_info_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libvmem.so.1"), tmpDir.resolve("lib/libvmem.so.1"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libwire_protocol_proto.so"), tmpDir.resolve("lib/libwire_protocol_proto.so"));
    Files.copy( ClassLoader.getSystemResourceAsStream("kudu-embedded/lib/libz.so.1"), tmpDir.resolve("lib/libz.so.1"));
  }
}
