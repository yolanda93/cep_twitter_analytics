package master2015;


import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;


public class trendingTopology
{
    private final Logger LOGGER = Logger.getLogger(this.getClass());
    private static final String KAFKA_TOPIC = "twitter-topic";
    private static final String ZOOKEEPER_URL = "localhost:2181";//"138.4.110.141:1003,138.4.110.141:1002";
    
    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();
        String[] languages;
        int windowAdvance;
        int windowSize;
        String topologyName;
        String folder;
        if (args != null && args.length>4)
        {
             
             languages=args[0].split(",");
             String[] windowParams=args[2].split(",");
             windowSize=Integer.parseInt(windowParams[0]);
             windowAdvance=Integer.parseInt(windowParams[1]);
             topologyName=args[3];
             folder=args[4];
           /*  StormSubmitter.submitTopology(
                args[3], // topology name
                createConfig(false),
                createTopology(languages));*/
            
          //Test in a local cluster
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(
                topologyName,
                createConfig(true),
                createTopology(languages,windowSize,windowAdvance,folder));
            Thread.sleep(60000);
            cluster.shutdown();
        }
        else{
         System.out.println("Arguments: langList, Zookeeper URL, winParams, topologyName, Folder");
         System.exit(0);
        }
    }

    private static StormTopology createTopology(String[] languages, int windowSize,int windowAdvance,String folder)
    {
         TopologyBuilder topology = new TopologyBuilder();
        SpoutConfig kafkaConf = new SpoutConfig(
            new ZkHosts(ZOOKEEPER_URL),
            KAFKA_TOPIC,
            "/kafka",
            "KafkaSpout");
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());     

        topology.setSpout("kafka_spout", new KafkaSpout(kafkaConf), 4);

        topology.setBolt("twitter_filter", new TwitterFilterBolt(languages), 4)
                .shuffleGrouping("kafka_spout");
        
       topology.setBolt("rolling-counter", new RollingCountBolt(30, 3), 4)
               .fieldsGrouping("twitter_filter", new Fields( "hashtag"));
        
        
       // topology.setBolt("total-ranker", new TotalRankingsBolt(3))
       //         .fieldsGrouping("rolling-counter", new Fields("tweet_lang","hashtag"));
        
       //topology.setBolt("hdfs", new HDFSBolt(), 4)
       //         .shuffleGrouping("score");
      //  topology.setBolt("nodejs", new NodeNotifierBolt(), 4)
       //         .shuffleGrouping("score");


        return topology.createTopology();
    }

    private static Config createConfig(boolean local)
    {
        int workers = 4;
        Config conf = new Config();
        conf.setDebug(true);
        if (local)
            conf.setMaxTaskParallelism(workers);
        else
            conf.setNumWorkers(workers);
        return conf;
    }
}