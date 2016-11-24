
################################## Amazon Dynamo Implementation #################################################################
This project is developed based on the paper - http://s3.amazonaws.com/AllThingsDistributed/sosp/amazon-dynamo-sosp2007.pdf

Implementation Details
-------------------------------------------------------------------------------------------
1. The coding is primarily done in Core Java, Java RMI and Google Protobuf
2. Used Gossip protocol for failure detection based on the paper - 
   https://www.cs.cornell.edu/home/rvr/papers/GossipFD.pdf
3. Used leveldb for the persistence at each node.
4. Used protobuf of version 2.5.0

Installations
---------------------------------------------------------------------------------------------
1. Install Java JDK which includes RMI too
2. Install Google protobuf of version 2.5.0 or higher
3. Install Maven
4. Install leveldb as shown below
------------------------------- Leveldb Installation steps -----------------------------------
wget https://leveldb.googlecode.com/files/leveldb-1.9.0.tar.gz
tar -xzf leveldb-1.9.0.tar.gz
cd leveldb-1.9.0
make
sudo mv libleveldb.* /usr/local/lib
cd include
sudo cp -R leveldb /usr/local/include
sudo ldconfig

How to start the project ?
--------------------------------------------------------------------------------------------
1. There has to be a directory /opt/data with full permissions. The leveldb data goes here.
2. Generate the protobuf java files if you want to add/edit the existing proto fields. Otherwise this step can be ignored.
   protoc -I=/ProtoFiles/ --java_out= src/protobuf/ ProtoFiles/dynamoProto.proto
   protoc -I=/ProtoFiles/ --java_out= src/protobuf/ ProtoFiles/loadBalancerProto.proto
3. Compile the source code using maven install
4. Start the LoadBalancer. Before starting it, provide the ip and port in the src file on which the lb will be running.
   java loadBalancer.LoadBalancer
5. Start each node has to be started using the below command. The RMI Registrys are started internally. The arguments to be passed or ip and port
  java -cp /home/pradeep/jars/protobuf-java-2.5.0.jar:/home/pradeep/jars/leveldbjni-all-1.8.jar:classes node.NodeImpl 10.0.0.1 1099.

