#AWS Microservices-based Spot Instance Batch Manager

##Overview
This Python project runs multiple concurrent batch processes on clusters of spot instances. This can result in significant savings for large clusters of servers running batch jobs, as spot prices are typically 10%-20% of the on-demand prices.  Termination detection and spot instance
resubmission are automatic and transparent.  Microservices are used to execute small async stateless chunks of
code, with state being retained in DynamoDB.  This is a message-driven architecture, where SQS messages contain the microservice name
in the header and they body contains JSON specific to that microservice.  All security resources (KeyPair, SecurityGroup, InstanceProfile, Role) are unique and dynamically created at the beginning of a batch job and deleted at batch job completion. For an overview of the architecture and an animated flow of a success state sequence, as well as a Spot Terminated state sequence, please see ppt/MicroservicesBasedSpotInstanceBatchMgr.ppt. 

The microservices have been designed for future deployment under AWS Lambda, whenever Lambda supports SQS.

This project contains a significant number of classes/methods which can be useful outside of this project, please feel free to copy/paste and
reuse in your projects.  For example, creating uniquely named SecurityGroups, SSH'ing into remote instances, Spot Instance Termination Detection adding a Policy to Role, etc.

This is the first version of this project, I would appreciate any and all feedback. The system, and it's installation, is non-trivial.  If you are going to install this project, feel free
to reach out to me and we can do a GoToMeeting where I will present the PPT and help you with the install, then update the installation instructions based on your feedback - pete.zybrick@ipc-global.com or pzybrick@gmail.com.  Once the installation has been vetted I'll create a script and/or CloudFormation template to automate the install.

##Installation
* Run the PPT to get an understanding of the state flow and parameter files
* Install awsext as per this https://github.com/petezybrick/awsext
* Open your IDE - for this example, PyDev 
* Download the awsspotbatch project from github into your IDE
* Run setup.py with the parms: sdist --formats-gztar
* Verify build succeeded: Gzip should be created: ./dist/awsext-1.1.tar.gz
* Copy awsext-1.1.tar.gz to target location (i.e. SCP to an EC2 instance)
* Login to target system and CD to target directory
* Execute the following commands:
	* sudo rm -rf awsext-1.1
	* tar -xvzf awsext-1.1.tar.gz
	* cd awsext-1.1
	* sudo python setup.py install
	* cd ..
	* sudo rm -rf awsext-1.1
* Verify successful installation
	* Start Python interactive and enter:
		* import awsspotbatch 
		* print awsspotbatch.Version
	* Should display "1.1" - if not, then research the error
	
##Setup
* Create DynamoDB tables
	* Review awsspotbatch.setup.createtables, accept the default table names or override
	* Run awsspotbatch.setup.createtables
* Create SQS Queues
	* Review awsspotbatch.setup.createqueues, accept the default queue names or override
	* Run awsspotbatch.setup.createqueues
* Create S3 bucket to contain awsext and awsspotbatch Python zips
	* Upload awsext-1-1.tar.gz and awsspotbatch-1-1.tar.gz to this bucket
	* Note that you will need to do this upload whenever the projects change
* Create Managed Policy
	* Review managedpolicy/SpotBatchMgrManagedPolicy.txt
	* If any SQS Queues or DynamoDB table names have changed than they must be updated in the policy document
	* In the AWS Console, bring up the IAM Service 
		* Create a new Managed Policy named SpotBatchMgrManagedPolicy
		* Copy/Paste the policy document from managedpolicy/SpotBatchMgrManagedPolicy.txt into the new managed policy
* Create Role for Microservices Dispatchers
	* Assign SpotBatchMgrManagedPolicy from above
* Create User and/or Group for local execution
	* Download the Access and Secret keys, create a new entry in your local .credential file, i.e. [spotbatchmgr-dev]
	* Assign SpotBatchMgrManagedPolicy from above
* Create 2x t2.micro instances to run the Dispatchers
	* Note that these instances will no longer be needed when the SpotManager is ported to AWS Lambda
	* Use Public AMI ami-29806542 SpotBatchMgrUbuntu20150523.  Note this AMI is in us-east-1, if you need it in another region, please copy it
	* These instances should be in separate AZ's
	* Create a KeyPair via the Console
	* Optionally create a SecurityGroup or reuse an existing SecurityGroup
		* Port 22 must be open for SSH
	* Create 2x t2.micro instances via the console.  This system has been tested on Ubuntu 14.04
		* Assign the Role and KeyPair created above, assign SecurityGroup as per above, accept all other defaults
* Configure the t2.micro instances
	* Do the following on each instance
		* Login via SSH
		* Note that the "spotbatchservice" will already be running (or at least attempted to)
		* Master Parm File
			* Review and optionally modify ./awsspotbatch/parm/masterservice.json to contain your primary region (where you created the SQS Queues and DynamoDB tables)  
		* Logs
			* review and optionally modify ./awsspotbatch/config/spotbatchservice.logconf to contain the log file directory location (must be absolute path)  
		* Restart the service
			service spotbatchservice restart
		* Tail the log and review for any error messages
			tail -f -n250 awsspotbatch/awsspotbatch.log
* Deploy latest code to t2.micro instances (do this initially and any time awsext or awsspotbatch projects are updated)
	* Ensure that setup.py has been run on awsext and awsspotbatch - should create new tar.gz's in the dist subdir
	* Get the Instance ID's of the 2 t2.micro instances
	* Review and modify deployment parms:
		* Windows: review/modify local file awsspotbatch/deployparm/deploy-master-windows-json
		* Linux: review/modify local file awsspotbatch/deployparm/deploy-master-windows-json
		* Modify the instance_ids list to include the instance id's of the 2 t2.micro instances
		* Modify the key_path_name_ext to point to the KeyPair .pem file created above for the t2.micro instances
		* Modify the client_bootstrap_local_path_name_ext, client_cmds_local_path_name_ext, awsext_zip_local_path_name_ext and awsspotbatch_zip_local_path_name_ext to use the Workspace path on the local system
	* Run the fulldeploy program:
		* You will need the Profile Name that was created above, i.e. [spotbatchmgr-dev]
		* Windows: awsspotbatch.deploy.fulldeploy <region_name> <profile_name> <workspacePath>/awsspotbatch/deploy-master-windows.json <workspacePath>/awsspotbatch/deployparm/deploycmds.txt
		* Linux: awsspotbatch.deploy.fulldeploy <region_name> <profile_name> <workspacePath>/awsspotbatch/deploy-master.json <workspacePath>/awsspotbatch/deployparm/deploycmds.txt

##Verify successful installation
* Open a browser session, open tabs for EC2 and DynamoDB services, optionally open for IAM and SQS
* Display Job Status (very crude, will improve in the future)
	* Run: awsspotbatch/status/displaystatus.py <path/name.ext of HTML file>, i.e. awsspotbatch/status/displaystatus.py /temp/spotstatus.html
	* Open the HTML file in a browser - the status will be refreshed about every 30 seconds
* Open an SSH session to each t2.micro instance and tail the logs
	tail -f -n250 awsspotbatch/awsspotbatch.log
* Run the "testsleep" sample job
	* 3 spot instances will be requested, each will run "testsleep.py" to sleep for 5 minutes
	* Run: 
* Monitor execution
	* Watch the browser session - you should see the Master and Requests start to process within a minute or so.  Watch the states change
	* Watch the logs - you will see each Message/Microservice, they all log messges with their UUID's
	* Periodically scan the DynamoDB spotbatch.master and spotbatch.request tables, you will see name/value pairs updated, added, state changing
	* Note that depending on the availability of Spot Instances, the process may take awhile - you will see the automatic termination resubmission in action
* Cleanup
	* Normally you can leave all of the items in the DynamoDB tables.  However, if you encounter errors and want to clear out those tables, then run awsspotbatch\test\unit\testdeletespotitems.py
	* Note that depending on any errors you encounter, you may want to also purge the Master and Request SQS Queues
		
		
##Any issues, please contact me at pete.zybrick@ipc-global.com or pzybrick@gmail.com, put "SpotBatch" in the subject....
