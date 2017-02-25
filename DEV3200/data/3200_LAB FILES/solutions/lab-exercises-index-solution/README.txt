Commands to run labs:

 // TODO part 1: in the DAO java objects replace user01 with the user name you are using.

Step 1: First compile the project: Select project 'lab-exercises' -> Run As -> Maven Install

Step 2: Copy the lab-exercises-index-solution-1.0.jar to the cluster

To run the solution exercises lab:
	java -cp `hbase classpath`:./lab-exercises-index-solution-1.0.jar social.SocialApp setup
to print the tables 	
	java -cp `hbase classpath`:./lab-exercises-index-solution-1.0.jar social.SocialApp print
to delete the tables 	
	java -cp `hbase classpath`:./lab-exercises-index-solution-1.0.jar social.SocialApp delete

