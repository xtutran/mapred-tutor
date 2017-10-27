How to run.
1. put a file into hdfs
 	hadoop fs -put src/test/resources/input/input.txt /user/cloudera/rawdata/input
 
2. bulid project
	mvn clean install
	
3. edit run.sh 
	vi src/main/resources/run.sh

	>>hadoop jar weight-vector-generator.jar com.mapred.main.MainDriver -conf config.xml <hdfs input path> <hdfs output path>
	example: hadoop jar weight-vector-generator.jar com.mapred.main.MainDriver -conf config.xml /user/cloudera/rawdata/input /user/cloudera/weightvector/output
	
4. source src/main/resources/run.sh

