javac -classpath libs/hadoop-core-1.2.1.jar -d compiled code/PageRank.java
jar -cvf compiled/compiled.jar -C compiled/ .
