javac -classpath libs/hadoop-core-1.2.1.jar -d compiled code/ProcessUnits.java
jar -cvf compiled/compiled.jar -C compiled/ .
