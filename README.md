# Mapreduce On Grid5000
With the help of : https://www.tutorialspoint.com/hadoop

### Use :
run the ```./init.sh``` command when connecting to the grid5000 serveur

run the ```./run.sh``` command will compile and run files


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
cd ${HOME}
wget ftp://mirrors.ircam.fr/pub/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar xvf hadoop-2.7.3.tar.gz
mv hadoop-2.7.3 hadoop
export HADOOP_HOME=${HOME}/hadoop

cd "repocloned"

cd libs
wget http://central.maven.org/maven2/org/apache/hadoop/hadoop-core/1.2.1/hadoop-core-1.2.1.jar
cd ..
```

### Hadoop on Grid5000
#### First Installation
```
export http_proxy="http://proxy:3128"; export https_proxy="https://proxy:3128"

easy_install --user execo

wget https://github.com/mliroz/hadoop_g5k/archive/master.zip .

echo "hadoop_g5k-master/" >> .gitignore

unzip master.zip
cd hadoop_g5k-master
python setup.py install --user
export PATH="$HOME/.local/bin:$PATH"
```
#### Everyday installation
```
oarsub -I -t allow_classic_ssh -l nodes=2,walltime=2
cd ~/tp/hadoop_g5k-master
python setup.py install --user
export PATH="$HOME/.local/bin:$PATH"
cd ~/

hg5k --create $OAR_FILE_NODES --version 2
//wget http://apache.crihan.fr/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
hg5k --bootstrap ~/hadoop-2.6.5.tar.gz
hg5k --initialize --start

hg5k --putindfs ~/tp/hadoop_input/* /input
//Vérification de la copie des inputs
hg5k --state files

hg5k --jarjob ~/tp/compiled/compiled.jar hadoop.PageRank /input /output 5
hg5k --getfromdfs /output5/* ./out_hdfs/
cat ./out_hdfs/output5/part-00000 | less
hg5k --state files

hg5k --delete
```
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

