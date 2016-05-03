#! /bin/bash

BUCKET="aws s3 cp s3://${1}/"
#masterScript
echo "#! /bin/bash" > masterScript.txt
echo "set -e -x" >> masterScript.txt
echo "${BUCKET}log4jM.properties ." >> masterScript.txt
echo "${BUCKET}master.jar ." >> masterScript.txt
echo -n "java -jar master.jar" >> masterScript.txt


#slaveScript
echo "#! /bin/bash" > slaveScript.txt
echo "set -e -x" >> slaveScript.txt
echo "${BUCKET}log4jS.properties ." >> slaveScript.txt
echo "${BUCKET}slave.jar ." >> slaveScript.txt
echo -n "java -jar slave.jar" >> slaveScript.txt
