buildAll:
	mvn clean package
start_master:
	java -jar Master/target/master.jar 2
start_slave:
	java -jar Slave/target/slave.jar 127.0.0.1
submit_job:
	java -jar MapReduce-API/target/mapred_api.jar
pseudo_run_Linux:
	./generateLauncher.sh 3
