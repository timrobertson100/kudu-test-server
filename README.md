# EXPERIMENTAL

This is a research project aiming to provide a `MiniKuduCluster` including _all_ 
dependencies (i.e. embedded binaries) runnable from a jar.

This should _only_ be used for writing tests against and is tested on a CentOS 7.4 only. 

To build: `mvn clean package`

To run: `java -cp kudu-test-server-0.1-SNAPSHOT.jar org.apache.kudu.embedded.TestEmbedded`