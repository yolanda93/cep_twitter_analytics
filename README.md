
# Author:
   Yolanda de la Hoz Simón. DNI: 53826071E. 

The startTwitterApp.sh script is a java application that read tweets from both the Twitter Streaming API (MODE 2) and preloaded log file (MODE 1). This application uses Apache Kafka to store the tweets and send them through a topic "twitter-topic" with a storm bolt as a consumer of this topic. 


# Usage:

    Arguments:

    • mode: 1 means read from file, 2 read from the Twitter API.
	• apiKey: key associated with the Twitter app consumer.
	• apiSecret: secret associated with the Twitter app consumer.
	• tokenValue: access token associated with the Twitter app.
	• tokenSecret: access token secret.
	• Kafka Broker URL: String in the format IP:port corresponding with the Kafka Broker
	• Filename: path to the file with the tweets (the path is related to the filesystem of the node that will be used to run the Twitter app)


# Start kafka
	
     1. Start Zookeeper server in Kafka using following script in your kafka installation folder  

     $bin/zookeeper-server-start.sh config/zookeeper.properties &

     2. Start Kafka server using following script 

     $bin/kafka-server-start.sh config/server.properties  &	


# Verify the Topic and Messages

    1. Check if topic is there using 
    
    $./kafka-topics.sh --list --zookeeper localhost:2181

    2. Consume messages on topic twitter-topic to verify the incoming message stream.
    
    $bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic twitter-topic --from-beginning


# Execute in cluster 05:
mvn clean compile package appassembler:assemble


scp -P 51005 -r appassembler masteruser1@138.4.110.141:/home/masteruser1
scp -P 51005 tweetsLogFile.log masteruser1@138.4.110.141:/home/masteruser1

scp -P 51002 trendingTopology.jar masteruser1@138.4.110.141:/home/masteruser1

4) Submit the topology to the cluster using the storm client, specifying the path to your jar, the classname to run, and any arguments it will use:

storm jar path/to/allmycode.jar org.me.MyTopology arg1 arg2 arg3


 # Connection to the cluster 
    your group ID is 10
    members: Yolanda De la Hoz Simon
    http://138.4.110.141:41002

    username: masteruser1
    password: 7Uljjbpb4

    ssh masteruser1@138.4.110.141 -p 51005 --> twitterApp
    ssh masteruser1@138.4.110.141 -p 51004 --> kafka


    ./kafka_2.10-0.8.2.1/bin/kafka-server-start.sh ./kafka_2.10-0.8.2.1/config/server.properties

    ./kafka-topics.sh --create --topic twitter-topic --zookeeper node2 --partitions 2 --replication-factor 1
    ./kafka-topics.sh  --describe --zookeeper node2  


    ssh masteruser1@138.4.110.141 -p 51002 --> storm H2

    ./zookeeper-3.4.6/bin/zkServer.sh start
    ./zookeeper-3.4.6/bin/zkServer.sh stop


    Nimbus: ./apache-storm-0.10.0/bin/storm nimbus
    UI: ./apache-storm-0.10.0/bin/storm ui
    Supervisor: ./apache-storm-0.10.0/bin/storm supervisor


    ssh masteruser1@138.4.110.141 -p 51003 --> storm H3

    Supervisor: ./apache-storm-0.10.0/bin/storm supervisor

    

# Read from file:
    
   ./startTwitterApp.sh "1" "../../tweetsLogFile.log" "node4:9092"

# Read from twitter API:

   ./startTwitterApp.sh "2" "FjFVvfxNkx3aqv2X0KYJKnxIP" "7IIB5CafrQIRlkHuO328KUQMgPlEjDtas3ciJYud7DdqTC0Kem" "475871668-W6d8hmIVwpjaypzxSDEmjrufRP58pJU5pKJvmNaR" "zFJUvJoOspsb39E2HcQsIQ6RfZBfYNuGGZhgQJ0gfJ9Df" "node4:9092" "../../tweetsLogFile.log" 
   
   -------Test Local-------
   "2" "FjFVvfxNkx3aqv2X0KYJKnxIP" "7IIB5CafrQIRlkHuO328KUQMgPlEjDtas3ciJYud7DdqTC0Kem" "475871668-W6d8hmIVwpjaypzxSDEmjrufRP58pJU5pKJvmNaR" "zFJUvJoOspsb39E2HcQsIQ6RfZBfYNuGGZhgQJ0gfJ9Df" "node4:9092" "../../tweetsLogFile.log"
