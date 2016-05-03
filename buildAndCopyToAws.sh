#! /bin/bash
rm -rf MasterIp
BUCKET_PATH=`grep "BUCKET_NAME=" user.config | cut -d'=' -f2`
N=`grep "NUMBER_OF_NODES=" user.config | cut -d'=' -f2`

if [ ! -z "$BUCKET_PATH" -a "$BUCKET_PATH" != " " ]; then
	mvn clean package
	aws s3 rm s3://${BUCKET_PATH}/master.jar || true
	aws s3 rm s3://${BUCKET_PATH}/log4jM.properties || true
	aws s3 cp Master/target/master.jar s3://${BUCKET_PATH}/ 
	aws s3 cp log4jM.properties s3://${BUCKET_PATH}/ 


	aws s3 rm s3://${BUCKET_PATH}/slave.jar || true
	aws s3 rm s3://${BUCKET_PATH}/log4jS.properties || true
	aws s3 cp Slave/target/slave.jar s3://${BUCKET_PATH}/
	aws s3 cp log4jS.properties s3://${BUCKET_PATH}/ 

	./masterSlaveScripts.sh ${BUCKET_PATH} ${N}

	java -jar Provisioning/target/provision.jar bootstrap.txt  masterScript.txt  slaveScript.txt ${N} ${BUCKET_PATH}
else
	echo "Error :: S3 Bucket path is not specified"
fi
