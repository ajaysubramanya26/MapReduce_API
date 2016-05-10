# MapReduce like API

## Introduction 
We are a team of four trying to build a MapReduce framework analogues to 
hadoop.
**Why?** : We have used hadoop extensively to write jobs that process huge
datasets, we understand how to use the hadoop’s APIs but we now are trying
to dig deeper and figure out what happens beneath the hood.We call this **PASS API** based on our initials.

## Prerequisites

* We are using Apache Maven 3.3.9 for dependency management and build automation.
* AWS CLI
* AWS credentials in '~/.aws/credentials'
* [AWS Instance profile](http://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html)
  should have full access to s3. the reason we use this is to not pass 
  credentials around while also granting the running EC2 instance full 
  access to s3.
* ```chmod +x pseudoSimulator.sh ``` & ```chmod +x buildAndCopyToAws.sh ```
* ```mvn clean package``` 
* Upload the appropriate jar file which has the mapper and 
  reducer logic (Ex : PassA3Redux.jar OR wordcount.jar) to S3 path and then follow
the below steps

## Porting from Hadoop MapReduce API to PASS API

* The driver class of mapreduce program should instantiate the Job class 
of PASS API instead of the Hadoop MapReduce API and set the required parameters 
the same as traditional Hadoop MapReduce API the only difference being instead 
of setting by class the PASS-API’s classes are set by class names as strings.

* The users mapper should extend the PASS API Mapper and Reducer classes and 
use the PASS API defined generic data types.

	| PASS API      | Hadoop MapReduce API          |
	| ------------- |:-------------:| 
	| CustomInteger | IntWritable   | 
	| CustomString  | Text          |
	| CustomLong    | LongWritable  |
	| CustomDouble  | DoubleWritable|

* Use the PASS API’s MapperContext, ReducerContext instead of Context object 
from Hadoop MapReduce API for mapper and reduce respectively.
* If Custom Key or Custom Value classes/objects are being defined, then 
those classes should implement Serializable, KeyInterface and Comparable. 
Also, the toString method should be overridden.

## Running job in Pseudo Distributed Mode

* Update the file user.config
	set value of NUMBER\_OF\_NODES, this will determine the number of Slaves
        that needs to be spawned. e. g if NUMBER\_OF\_NODES= 3, it would spawn 1
        Master process and 2 slave processes on the same machine in different
        terminal windows
* Run the script ```./pseudoSimulator.sh```
* It will start a Terminal and start Master Bootstrap, once Master Bootstraps,
   Press Enter key, it will spawn the slave terminals
* Once you get the confirmation, run the user jar
* Output will be stored on S3 path

## Running job in Distributed Mode

* Update the user.config file with NUMBER\_OF\_NODES and BUCKET_NAME
* Update bootstrap.txt with the details of user running the script
* run script ```./buildAndCopyToAws.sh```
* After the provesioning is complete, the script will prompt for user input
   enter the java command to be run
* After success / Error , script will terminate the spawned ec2 instances
* Output will be stored on S3 path

## Design 

### Brainstorming Design
Design based on YARN approach and Hadoop MapReduce API. Before coming up 
with the design for implementing MapReduce framework we brainstormed. 
For running on distributed system we tried to understand YARN ‘s 
components and how they start master and slaves and interact with each 
other. For understand the MapReduce Hadoop API we refered online sources
and understood all the methods that our PASS MapReduce API needs to implement. 

### System Design

Main actors in our PASS MapReduce model are as follows:
Local Machine/Any Machine: To run user jar on local(pseudo setup mode) 
or ec2(cloud mode where provisioning setup is provided) S3: shared FS
Master & Slave machines



![Doh System Design daigram didn't load](https://github.com/ajaysubramanya26/MapReduce_API/blob/master/markdown_images/SystemDesign.png "System Design") 

### Activity Diagram/Workflow



![Doh Activity diagram didn't load](https://github.com/ajaysubramanya26/MapReduce_API/blob/master/markdown_images/ActivityDiagram.png "Activity Diagram") 

## Building Blocks

### MapReduce-API
	
* **Job** : The job class is the main point of entry to the API for the 
  user mapreduce program. It has two methods - submit and waitForCompletion.

* **submit** : accepts a user job and models it into a Job object and sends
  it to the MRAppMaster. Here the main components are validation and network
  communication. Validation is making sure the inputs and the parameters 
  supplied by the user are as required by the contract of the API. Once 
  the validation is complete we then send the job over the network 
  (not exactly if we are running in pseudo) to the master. Here we connect 
  to the master and send the validated job object. 
  
* **waitForCompletion** : Once the job is submitted to the master - it 
  polled from time to time to check for the status of the job. How this 
  works is the master keeps the API informed of the status as and when it 
  changes , but if the API does not hear from master for over a minute the 
  API sends a status request to the master, and the master replies with the 
  status. 

* **Mapper** :  This is a class extended by User Defined Mapper which 
  provides similar functionality as Hadoop Mapper class, Setup, map, and 
  cleanup. It also has run method which is called by slave for assigning 
  a specific Map Task.

* **MapperContext** : MapperContext object is used by each Mapper instance, 
  which will hold the references for reader object, that reads the data 
  from the input file, generates a Long key and returns the key and read 
  data row to the map() method where it is processes.Writer object, this 
  is used when map() / cleanup() calls write with key and value to write 
  the map() output to be used by reducer phase. Writer writes the objects 
  instead of simple text, as this facilitates the read operation in 
  ReduceContext

* **Reducer** :  This is a class extended by User Defined Reducer which 
  provides similar functionality as Hadoop Reducer class, Setup, map, 
  and cleanup. It also has run method which is called by slave for 
  assigning a specific Reduce Task.

* **ReducerContext** : ReducerContext object is sued by each reducer 
  instance, which will hold references for all the related files for 
  a particular key on which current reduce() is being performed

* **Reader** : for each key, MapperContext() writes the data in a 
  key -> List<Values> format. Reader reads the files for a single key, 
  and consolidates all the values and passes its Iterable to the the 
  reduce method where all the values are reduced in either user defined 
  reduce(), or the reduce() method defined in Mapper. 
  
* **Writer** : this object holds the reference to a local file 
  part-r-0000X that will be used to write the output of current 
  reduce(). After reduce is finished, this method writes the key 
  as a string and value as a String to this file.

* **Custom Data Types** : Similar to the Hadoop writable objects, 
  we have defined our own primitive datatypes, to be used by the API 
  and to ease the porting of existing Hadoop codes to suite to PASS API.

### MRAppMaster

* Main class of the Master. MRAppMaster spawns 2 servers i.e. JobServer 
  and SlaveServer. SlaveServer opens a netty server socket to communicate 
  with Slaves. JobServer opens a netty server socket to listen to incoming 
  job requests.Master and Slaves are up and running and established 
  communication before a user can submit the job. If user submits the 
  job any time before error message is thrown.

* To handle the job effectively 3 state machines are implemented.

	* JobStateObserver : Maintains the different phases during running 
	  the Job(Submitted, Map,  Shuffle, Reduce, UploadLogs, Finished, Error)

	* SlaveServerStateMachine: Maintains all the SlaveChannels, 
	  JobSubmitterChannel and SlaveHeartBeatInfo table. When channel is 
	  idle heartbeat of 60 secs from every slave is received and Master 
	  keeps tracks of the active slaves.

	* TaskStateMachine: Used to store, track, schedule, scheduleNext, update the 
	  tasks within a phase. Each phase is associated with List<Tasks> and 
	  TaskTracker. 

* SlaveServer and slaves waits till Job Server receives Job. Once the 
  JobServer receives Job it connects to SlaveServer and sends a Job object. 
  Input Split is obtained and a Queue of MapTasks are created for each input 
  split. Then MapTask tracker is setup to track the time and status for each 
  task. First set of Map tasks is sent to slaves and removed from Queue. 
  Slave executes the task and replies with the TaskResult object and task 
  tracker is updated for the taskid and next task in the queue is scheduled. 

  Once all the map tasks are finished total time for map tasks is computed 
  and logged. Begin the shuffle phase. We are using s3 as a shared filesystem. 
  Obtain metadata of all the files in MapTemp output and get the keys form the
  filenames. Sort the keys by forming mapKeyOutputClass objects. Time taken 
  for this is logged. Now we have a set of keys and reduce tasks are formed 
  and reduce task tracker is setup. First set of reduce tasks from the Queue 
  is dispatched to slaves and upon receiving TaskResult task tracker is 
  updated for the taskid and next task is the Queue is scheduled.
	
  Once all the reduce tasks are finished successfully _SUCCESS file is 
  uploaded and time taken is logged and  Upload log Task is sent to 
  slaves and on success master writes its log and sends finished status 
  to JobSubmitter and closes slave channels and jobsubmitter channel and 
  finally itself.
	 
  On error in any of the above tasks then error is returned to the user.
  
### Slave 

This is used by the master to execute tasks. A task may be a Map Task, 
Reduce Task or Upload Logs task. There is a channel established between 
each Slave with the master. The master sends down a task object every 
time it wants to some job done. The Slaves understand the task object 
and do the necessary task that the master requested and write the result 
to s3 and inform master about the success or failure of a task. 

There are mainly three parts to the slave - Handlers, Java Reflection 
and S3 Wrapper

* Handlers : There are mainly three handlers - SlaveHandler, MapHandler, 
  ReduceHandler and finally LogHandler. Slave handler is used to send 
  the initial message to the master saying that it was spawned and ready 
  for tasks. 
  
* Java Reflection : Used to load the user jar dynamically and then load 
  the user written mapper or reducer class and then the methods in those 
  classes. 
  
* S3 Wrapper : Used by the slave to write intermediate and final results 
  to s3. 

### Common

Is used to hold class that are or have the potential to be used in more 
than one module. This is added as a dependency in all the modules. The 
most significant Class in this would be ‘Task’ which models a task that 
the master sends down to the slave.

### Provisioning 

This takes care of spawning the EC2 machines and tagging them as master 
and slaves. It initiates the network communication between them. 
Additionally, it makes sure to run the respective scripts on the master 
and the slave machines. These scripts downloads the respective jar and 
log4j.properties file from S3 and runs the jar with the appropriate 
paramaters. Once this is done, the Provisioning will ask the user on 
his local machine to input the command to run their mapreduce program. 
This program is run in a separate JVM on the local machine and the 
mapreduce tasks are performed on the EC2 cluster spawned before. Once 
the mapreduce tasks are completed with a success or failure, the 
EC2 instances are terminated. However, the logs are uploaded to AWS S3 
and can be viewed in case something goes wrong.

## Why use netty.io for Network communication ?

We initially thought that we may have to handle huge volumes of data 
flowing between components. And somehow using plain sockets did not 
appeal to us. We did not want to go through the pain of debugging 
network logs and working at socket/TCP level. We were looking for a 
library that abstracted these concepts and just gave us methods to read 
and write data from a channel. We were introduced to netty in the paper 
**“Scaling spark in the real world - Performance and Usability ” by 
Michael Armbrush et al.** They were using netty for the following 
reasons.

* **Zero copy I/O** : Instruct the kernel to copy data directly from on-disk 
files to the socket, without going through the user-space memory. This 
reduces not only the CPU time spent in context switches between kernel 
and user space, but also the memory pressure in the JVM heap.Off-heap 

* **Network buffer management** : Netty maintains a pool of memory pages 
explicitly outside the Java heap, and as a result eliminates the impact 
of network buffers on the JVM garbage collector.

* **Multiple connections** :  Each Spark worker node maintains multiple 
parallel active connections (by default 5) for data fetches, in 
order to increase the fetch throughput and balance load across the 
nodes serving data.

The above features impressed us , and we started digging deeper into 
understanding netty. To be honest it was a steep task as there are 
limited tutorials, except for the ones by the developers of netty which 
is limited. Although the examples presented by them was helpful for us 
in a way that we were able to model our client server architecture using 
it. But beyond that using handlers and various other functionalities we 
had to refer the API docs and figure things out. 

We started out building a POC which later transformed into our 
A9 - distibuted project. Since we were comfortable with it after a9 we 
continued using it for the project, although we may not be leveraging 
all of the above mentioned features, but it gave us a clean framework 
to fit in our components.

## What works, and what doesn't ?

### What works ?

* Basic MapReduce functionality with framework to run the jobs on 
distributed framework.
* Having the API work for pseudo-distributed and distributed setup based 
on user preference.
* Successfully able to run WordCount and A3 with minimal modifications 
to existing hadoop programs.
* HeartBeat mechanism from slaves so master knows which slaves are active.
* JobSubmitter receive status of the Job from MRAppMaster and MRAppMaster 
informs about the state changes.
* Time taken for each task is tracked and logged along with the total time 
to complete all the tasks in a phase.
* Logs uploaded to s3 at the end.

### What doesn't ?

* Combiner
* Chaining jobs together
* Split a large file into manageable block size
* Replication of intermediate data 
* Retry mechanism(But can be easily implemented with the framework we 
have built and data is stored in s3)
* Shuffle phase should ideally begin after 80% of Mapper phase . In Our 
API it waits till Map phase is completed
* Composite Object as a key

## Enhancements

* In the reducer we are currently reading all the files for a key and 
providing the values for the key as an iterable to the reduce method. 
This results in memory overhead since we are holding all the values for 
a key in the memory. This can be improved by reading and passing every 
object/row from the file to the reduce method one at a time, instead of 
loading all the objects/rows in memory and then passing it to the reduce 
method. 
* Writing generic interfaces for all user defined data types to be 
implemented. This will enable setting up a contract that a user-defined 
data type ,i.e., Composite Key or Composite Value has to follow.

* **Using HDFS over S3 as shared FS**

	One of the major enhancements we could possibly do is use HDFS as a 
	shared file system over S3. Using S3 has the following drawbacks , 

	* The user of the API should have an AWS account, if not the API would 
	not be very useful. 
	* For huge datasets there would be considerable amount of network traffic. 
	HDFS also would have network traffic but that would be within the intranet 
	and not over public internet. 
	* If S3 is down for some reason then the API would not work at all. 
	* Lots of configurations needed for authentication and access. 

	While there are a lot of issues there are some things that go it it’s 
	favor, 

	* It was easy for us to setup and use s3 , the java SDK that AWS provides 
	does work decently well. 
	* Since we are not implementing replication of data and having fixed block 
	sizes , it was just used as a shared file system , and nothing more. We wrote 
	output of a phase to it and read it back in the next phase.
	* Since we were delegating the task of data transfer to AWS S3 SDK, our 
	implementations of mapper and reducers were simplified to a great extent. 
	This in comparison to our distributed sort implementation 
	(A9 - previous assignment) reduced lines of code by at least half. 

	We did look into the API’s that HDFS provides for data transfer and storage, 	org.apache.hadoop.fs.FileSystem  is a generic class to access and manage HDFS 
	files/directories located in distributed environment. This could be accomplished 
	as one of the future work/enhancement to this project which would allow for users 
	to run their mapreduce jobs using our API in pseudo-distributed mode.
	
## Conclusion

We would like to conclude that working on this project made us appreciate the 
effort that going into building a system like hadoop. We have tried to simplify 
the implementations wherever possible, such as not having equal inputs splits or 
data replication. Although, we may have a simple implementation of hadoop API, we 
now know where the the design improvements that need to be done and how they could 
be done. For instance we already have a framework for implementing retry mechanism, 
composite key and Job Chaining but did not finish it due to time constraints.

## Authors 

* _Smitha Bangalore Naresh_ : **bangalorenaresh{dot}s{at}husky{dot}neu{dot}edu**
* _Prasad Memane_  : **memane{dot}p{at}husky{dot}neu{dot}edu**
* _Swapnil Mahajan_ :  **mahajan{dot}sw{at}husky{dot}neu{dot}**
* _Ajay Subramanya_ : **subramanya{dot}a{at}husky{dot}neu{dot}edu**