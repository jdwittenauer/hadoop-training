Commands to run labs:

Step 1: First compile the project: Select project 'lab-exercises-shopping' -> Run As -> Maven Install
Step 2: Copy the lab-exercise-shopping-1.0.jar to the cluster

To run the lab:
    java -cp `hbase classpath`:./lab-exercises-shopping-solution-1.0.jar shopping.ShoppingCartApp init
    java -cp `hbase classpath`:./lab-exercises-shopping-solution-1.0.jar shopping.ShoppingCartApp delete Mike
    export LD_LIBRARY_PATH=/opt/mapr/hadoop/hadoop-0.20.2/lib/native/Linux-amd64-64
    