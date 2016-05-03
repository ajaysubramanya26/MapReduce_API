#######################################################################
# Author : Swapnil Mahajan,             		              #
# Description : Script to simulate the pseudo distributed environment # 			
#               on Linux                                              #
#######################################################################
#!/bin/bash 
 COUNTER=0
 N=`grep "NUMBER_OF_NODES=" user.config | cut -d'=' -f2`
 let N=N-1
 PWD=`pwd`
 echo "#!/bin/bash" > launcher.sh
 echo 'xterm -title "Master" -hold -e "cd '$PWD' ; make buildAll ; make start_master" &' >> launcher.sh
 echo 'echo Wait for Master to bootstrap fully and then Press Enter' >> launcher.sh
 echo 'echo "look for message bootstraping job receive server"' >>launcher.sh
 echo 'read var' >> launcher.sh

 while [ $COUNTER -lt $N ]; do
      echo 'xterm -title "Slave '$COUNTER'" -hold -e "cd '$PWD' ; make start_slave" &' >> launcher.sh
      echo "sleep 5" >> launcher.sh
     let COUNTER=COUNTER+1 
 done
echo "echo \"Master and Slaves are Up and Running\"" >> launcher.sh
echo "echo \"Please enter the job now like : java -jar <Local jar path> [Arguments to the Jar]\"" >> launcher.sh
echo "echo \"Example : java -jar A3/target/PassA3Redux.jar -s3JarPath=s3://swapnilmapreduce/PassA3Redux.jar -input=s3://swapnilmapreduce/Airline -output=s3://swapnilmapreduce/MRPassOutput -calculate=e\"" >> launcher.sh
chmod 755 launcher.sh
./launcher.sh
