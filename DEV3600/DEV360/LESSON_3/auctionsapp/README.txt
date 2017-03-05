Steps to run a spark application:

Step 1: Compile the project into a JAR file
Step 2: scp auctionsapp-1.0.jar user01@ipaddress:/user/user01/.
Step 3: spark-submit --class solutions.AuctionsApp --master yarn auctionsapp-1.0.jar
