### How to run.
1. put a file into hdfs
 	hadoop fs -put src/test/resources/input/input.txt /user/cloudera/rawdata/input
 
2. build project
	mvn clean install
	
3. edit run.sh 
```bash
vi src/main/resources/run.sh
```

```bash
hadoop jar target/secondary-sort.jar com.mapred.main.MainDriver -conf config.xml <hdfs input path> <hdfs temp path> <hdfs output path>

#example: 
hadoop jar target/secondary-sort.jar com.mapred.main.MainDriver -conf config.xml /user/cloudera/rawdata/input /user/cloudera/weightvector/output-count /user/cloudera/weightvector/output
```

4. run your shell script
```bash
source src/main/resources/run.sh
```

