################################################################################
# README-MapReduce Project
# Smitha Bangalore Naresh, Prasad Memane, Swapnil Mahajan, Ajay Subramanya

# bangalorenaresh{dot}s{at}husky{dot}neu{dot}edu
# memane{dot}p{at}husky{dot}neu{dot}edu
# mahajan{dot}sw{at}husky{dot}neu{dot}edu
# subramanya{dot}a{at}husky{dot}neu{dot}edu
################################################################################

################################  PREREQUISITES ################################

* We are using Apache Maven 3.3.9 for dependency management and build automation

* AWS CLI

* AWS credentials in '~/.aws/credentials'

* AWS Instance profile #link http://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html 
  the above instance profile should have full access to s3. the reason we use 
  this is to not pass credentials around while also granting the running EC2 
  instance full access to s3

* 

################################## RUN SCRIPT ##################################
Make sure that correct execute permissions are given to .sh files

run  : mvn clean package inside the folder mapreduce-project

Please upload the PassA3Redux.jar OR wordcount.jar to S3 path and then follow 
the below steps

* For running in pseudo mode (Local Machine)
1. Update the file user.config
	set value of NUMBER_OF_NODES, this will determine the number of Slaves 
        that needs to be spawned. e. g if NUMBER_OF_NODES= 3, it would spawn 1
        Master process and 2 slave processes on the same machine in different 
        terminal windows

2. Run the script ./pseudoSimulator.sh

3. It will start a Terminal and start Master Bootstrap, once Master Bootstraps,
   Press Enter key, it will spawn the slave terminals

4. Once you get the confirmation, run the user jar

5. Output will be stored on S3 path

* For running on AWS mode (EC2 Machines)

1. Update the user.config file with NUMBER_OF_NODES and BUCKET_NAME

2. Update bootstrap.txt with the details of user running the script

3. run script ./buildAndCopyToAws.sh

4. After the provesioning is complete, the script will prompt for user input 
   enter the java command to be run

5. After success / Error , script will terminate the spawned ec2 instances 

6. Output will be stored on S3 path

################################## CAVEATS #####################################

* Project structure is as below,

MapReduce
├── Common
│   ├── pom.xml
│   ├── src
│   └── target
├── MapReduce-API
│   ├── pom.xml
│   ├── src
│   └── target
├── Master
│   ├── pom.xml
│   ├── src
│   └── target
├── Provisioning
│   ├── pom.xml
│   ├── src
│   └── target
├── Slave
│   ├── pom.xml
│   ├── src
│   └── target
├── pom.xml
└── src
    └── site

+ MapReduce is the parent maven project, under which we have Common, Master, 
  Slave, provisioning, MapReduce API as child maven modules. The parent 
  project is mainly an umberalla project which does nothing except that we 
  could use it to include dependencies which we may use in all children modules.
  Also all child modules will be built from the parent project.

+ Note that each project has a pom, where we add dependencies specific to that 
  project.

+ Common : is a project which *should* only have common classes that are shared 
  between Master and Slave. 

+ MapReduce-API : Should contain all the Hadoop API's which we would implement. 
  The JAR extracted from this would be placed in the user project in place of
  the hadoop API JAR.  

+ Master : Would be the master program which would be running a server and 
  listining to clients. 

+ Slave : the slave code would sit here and would connect to the master and 
  receive task objects. It would work on the task objects and write result 
  back to the master. 

+ Provisioning : Would have all the code that would be used to provision the 
  nodes in the cluster. 


* We are using netty.io for network communication between the various components
  listed above. 

################################### THE END ####################################
