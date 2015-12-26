package master2015;


import backtype.storm.Config;


import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 * Top3App
 * Created on Dec 22, 2015
 * @author Yolanda de la Hoz Simon <yolanda93h@gmail.com>
 */
public class Top3App
{
    private final Logger LOGGER = Logger.getLogger(this.getClass());
   
    
    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();
        String[] languages;
        int window_advance;
        int window_size;
        String topology_name;
        String folder;
        String zookeper_url;
        if (args != null && args.length>4)
        {
             
        	   languages=args[0].split(",");
             zookeper_url=args[1];   
             String[] window_params=args[2].split(",");
             window_size=Integer.parseInt(window_params[0]);
             window_advance=Integer.parseInt(window_params[1]);
             topology_name=args[3];
             folder=args[4];
              StormSubmitter.submitTopology(
                topology_name, // topology name
                createConfig(false),
                createTopology(languages,zookeper_url,window_size,window_advance,folder));
            
          //Test in a local cluster
                /*     LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(
                topology_name,
                createConfig(true),
                createTopology(languages,zookeper_url,window_size,window_advance,folder));
            Thread.sleep(90000);
            cluster.shutdown();*/
        }
        else{
         throw new IllegalArgumentException("Arguments: langList, Zookeeper URL, winParams, topologyName, Folder");
        }
    }

    private static StormTopology createTopology(String[] languages,String zookeper_url, int windowSize, int windowAdvance, String folder) throws IOException {
        TopologyBuilder topology = new TopologyBuilder();
       
       topology.setSpout("kafka_spout", new KafkaConsumer(zookeper_url,"kafkaSpout"), 2);

       topology.setBolt("twitter_filter", new TwitterFilterBolt(languages), 2)
                .fieldsGrouping("kafka_spout", new Fields("tweet"));
        
       topology.setBolt("window-counter", new HashtagCountBolt(windowSize, windowAdvance))
               .fieldsGrouping("twitter_filter", new Fields("hashtag"));

       topology.setBolt("ranking-result", new RankerBolt())
             .fieldsGrouping("window-counter", new Fields("lang"));

       topology.setBolt("output-result", new OutputToFileBolt(languages,folder))
               .fieldsGrouping("ranking-result", new Fields("lang"));

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