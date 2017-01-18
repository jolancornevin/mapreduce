echo '--- ./compile.sh ---'
./compile.sh
echo '--- rm hadoop_input/sample.txt ---'
rm hadoop_input/sample.txt
echo '--- $HADOOP_HOME/bin/hadoop fs -put ./input/sample.txt hadoop_input/ ---'
$HADOOP_HOME/bin/hadoop fs -put ./input/sample.txt hadoop_input/
echo '--- rm -r hadoop_output  ---'
rm -r hadoop_output
echo '--- $HADOOP_HOME/bin/hadoop jar compiled/compiled.jar hadoop.ProcessUnits hadoop_input/ hadoop_output/out 20  ---'
$HADOOP_HOME/bin/hadoop jar compiled/compiled.jar hadoop.PageRank hadoop_input/ hadoop_output/out 40
echo '--- ./see_output.sh --'
./see_output.sh
