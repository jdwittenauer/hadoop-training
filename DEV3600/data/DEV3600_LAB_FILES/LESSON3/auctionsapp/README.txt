Commands to run labs:

Step 1: First compile the project on eclipse: Select project  -> Run As -> Maven Install
Netbeans: Select project  -> build

Step 2: use scp to copy the jar from the target folder to the mapr sandbox or cluster

scp  auctionsapp-1.0.jar  user01@ipaddress:/user/user01/.
if you are using virtualbox:
scp -P 2222 auctionsapp-1.0.jar  user01@127.0.0.1:/user/user01/.

To run the lab:

Step 3: 

spark-submit --class solutions.AuctionsApp --master yarn auctionsapp-1.0.jar 




