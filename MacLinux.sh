#######################################################################
# Author : Swapnil Mahajan,Prasad Memane      		              #
# Description : Script to simulate the pseudo distributed environment # 			
#               on OSX                                                #
#######################################################################

#!/bin/bash 
COUNTER=0
PWD=`pwd`
N=`grep "NUMBER_OF_NODES=" user.config | cut -d'=' -f2`
let N=N-1
rm -rf launcherMac.sh
touch launcherMac.sh

 echo "#!/bin/bash" > launcherMac.sh
 echo "osascript -e 'tell app \"Terminal\"" >> launcherMac.sh
 echo "do script \"cd  $PWD ; mvn clean package && make start_master &\"" >> launcherMac.sh
 echo "end tell'" >> launcherMac.sh
 
 echo 'echo Wait for Master to bootstrap fully and then Press Enter' >> launcherMac.sh
 echo 'echo "look for message : bootstraping job receive server"' >>launcherMac.sh
 echo 'read var' >> launcherMac.sh

 while [ $COUNTER -lt $N ]; do	
       echo "osascript -e 'tell app \"Terminal\"" >> launcherMac.sh
       echo "do script \"cd  $PWD ; make start_slave &\"" >> launcherMac.sh
       echo "end tell'" >> launcherMac.sh
       echo "sleep 5" >> launcherMac.sh
     let COUNTER=COUNTER+1 
 done
echo "echo \"Master and Slaves are Up and Running\"" >> launcherMac.sh
echo "echo \"Please enter the job now like : java -jar <Local jar path> [Arguments to the Jar]\"" >> launcherMac.sh
echo "echo \"Example : java -jar A3/target/PassA3Redux.jar -s3JarPath=s3://swapnilmapreduce/PassA3Redux.jar -input=s3://swapnilmapreduce/Airline -output=s3://swapnilmapreduce/MRPassOutput -calculate=e\"" >> launcherMac.sh
chmod 755 launcherMac.sh
./launcherMac.sh
