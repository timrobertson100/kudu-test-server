package org.apache.kudu.embedded;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility to provide binaries allowing a {@link org.apache.kudu.client.MiniKuduCluster} to run.
 *
 * <p>This class will unpack the binaries into a temporary folder, allowing them to be executable,
 * and then will register the hooks so they are deleted on JVM shutdown. A <code>kill -9</code> may
 * result in temporary directories not being deleted.
 *
 * <p>To use in a <code>JUnit</code> one should do the following:
 * <pre>{@code
 *
 * private static MiniKuduCluster miniCluster;
 *
 * @BeforeClass
 * public static void setup() throws Exception {
 *   EmbeddedKudu.prepare();
 *   miniCluster = new MiniKuduCluster.MiniKuduClusterBuilder().numMasters(1).numTservers(1).build();
 * }
 *
 * @AfterClass
 * public static void teardown() throws Exception {
 *   miniCluster.shutdown();
 * }
 * }</pre>
 */
public class EmbeddedKudu {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedKudu.class);

  public static void main(String[] args) throws Exception {
    prepare();
  }

  /**
   * Extracts the Kudu binaries into a temporary folder and sets the system property "binDir" ready for the minicluster to be used.
   * All extracted files and directories created will be deleted on JVM shutdown.
   *
   * @throws Exception On error extracting the files or creating the temporary directory
   */
  public static void prepare() throws Exception {
    Path tmpDir = Files.createTempDirectory("kudu");
    tmpDir.toFile().deleteOnExit();
    LOG.info("Extracting Kudu binaries to {}", tmpDir);

    tmpDir.resolve("bin").toFile().mkdir();
    tmpDir.resolve("lib").toFile().mkdir();
    tmpDir.resolve("bin").toFile().deleteOnExit();
    tmpDir.resolve("lib").toFile().deleteOnExit();

    List<String> files = listFiles("/kudu-binaries", tmpDir);
    for (String f : files) {
      LOG.info("Extracting {}", f);
      String source = "kudu-binaries/" + f;
      Files.copy( ClassLoader.getSystemResourceAsStream(source), tmpDir.resolve(f));
      if (f.startsWith("bin")) {
        tmpDir.resolve(f).toFile().setExecutable(true);
      }
      tmpDir.resolve(f).toFile().deleteOnExit();
    }

    // used by the minicluster
    System.setProperty("binDir", tmpDir.resolve("bin").toString());
  }


  /** Lists the files to copy supporting both IDE use and when in Jar */
  private static List<String> listFiles(String source, Path tmpDir) throws IOException, URISyntaxException {
    URI sourceUri = EmbeddedKudu.class.getResource(source).toURI();
    Path sourcePath;
    FileSystem fileSystem = null;
      if (sourceUri.getScheme().equals("jar")) {
        fileSystem = FileSystems.newFileSystem(sourceUri, Collections.emptyMap());
        sourcePath = fileSystem.getPath(source);
      } else {
        sourcePath = Paths.get(sourceUri);
      }

      try (Stream<Path> stream = Files.walk(sourcePath, 2)) {
        return stream
            .filter(Files::isRegularFile) // skip the directory
            .map(f -> sourcePath.relativize(f).toString())
            .collect(Collectors.toList());
      }
  }
}
