package stormTopology;

import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * KafkaConsumer
 * Created on Dec 22, 2015
 * @author Yolanda de la Hoz Simón <yolanda93h@gmail.com>
 */
public class KafkaConsumer extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private ConsumerConnector consumer;
    private SpoutOutputCollector collector;
    String a_zookeeper;
    String a_groupId;
    private static final String KAFKA_TOPIC = "twitter-topic";

    public KafkaConsumer(String a_zookeeper, String a_groupId) {
        this.a_zookeeper = a_zookeeper;
        this.a_groupId = a_groupId;
    }

    /**
     * Method to declare the output fields
     * @param ofd tweets received
     */
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("tweet"));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }
    
    /**
     * Method to create the neccesary configuration to consume the kafka topic
     * @param a_zookeeper the zookeeper url
     * @param a_groupId  the group id
     */
    private kafka.consumer.ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    /**
     * Method that creates the consumer connector.
     * @param map
     * @param tc
     * @param soc
     */
    @SuppressWarnings("rawtypes")
	public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        this.collector = soc;
    }

    /**
     * Method that receives and emits tweets
     */
    public void nextTuple() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KAFKA_TOPIC, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap
                = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(KAFKA_TOPIC);
  
        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        while (it.hasNext()) {

            collector.emit( new Values(new String(it.next().message())));
        }
    }

}
