# mapreduce
With the help of : https://www.tutorialspoint.com/hadoop

## Initialisation : 
```
cd ~
alias ll='ls -la'
```

### Installing java
```
sudo-g5k apt install default-jdk
export JAVA_HOME=/usr/lib/jvm/default-java
```

### Installing hadoop
```
wget ftp://mirrors.ircam.fr/pub/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar xvf hadoop-2.7.3.tar.gz
mv hadoop-2.7.3 hadoop
export HADOOP_HOME=${HOME}/hadoop

cd libs
wget http://central.maven.org/maven2/org/apache/hadoop/hadoop-core/1.2.1/hadoop-core-1.2.1.jar
cd ..
```

### Init hadoop
## How to use it :

### Directory :
#### Code : 
Contains all our code

#### Compiled :
Contains all the compiled files, themself compiled with the compile.sh command

#### hadoop_input :
Is the input directory assign to hadoop. Generated with the following command 
```$HADOOP_HOME/bin/hadoop fs -mkdir hadoop_input/```

#### input : 
Is our input directory.
```$HADOOP_HOME/bin/hadoop fs -put ./input/sample.txt```

### Use :
run the ```./run.sh``` command will compile all files and run them on the inputs files
