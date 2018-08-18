# EXPERIMENTAL

This is a research project aiming to provide a `MiniKuduCluster` including _all_ 
dependencies (i.e. embedded binaries) runnable from a jar.

This should _only_ be used for writing tests against and is tested on a CentOS 7.4 only. 

To build: `mvn clean package -Plinux` (Only the linux profile exists today)

To run: `java -cp kudu-embedded-example/target/kudu-embedded-example-0.1-SNAPSHOT.jar  org.apache.kudu.demo.EmbeddedKuduDemo`

The project contains:
1. `kudu-embedded` containing binaries built into Jars with a classifier, and a single class to extract those at runtime.
2. `kudu-embedded-example` showing how a jar can be included and an example run.