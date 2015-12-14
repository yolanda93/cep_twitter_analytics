package master2015;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Map;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.reflect.TypeReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TwitterFilterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    String[] lang;

    public TwitterFilterBolt(String[] languages) {
        this.lang = languages;
    }

    private boolean isValidLang(String tweet_lang) {
        for (int i = 0; i <= lang.length - 1; i++) {
            if (lang[i].equals(tweet_lang)) {
                return true;
            }
        }
        return false;
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        LOGGER.debug("Filttering incoming tweets");
        String json = input.getString(0);
        try {
            JsonNode root = mapper.readValue(json, JsonNode.class);
            long timestamp;
            String tweet_lang;
            String hashtag;
            if (root.get("lang") != null && isValidLang(root.get("lang").textValue())) {
                if (root.get("timestamp_ms") != null && root.get("entities").get("hashtags") != null) {
                    timestamp = root.get("timestamp_ms").asLong();
                    tweet_lang = root.get("lang").textValue();
                    String array = root.get("entities").get("hashtags").toString();
                    if (array.length() > 0) {
                        ArrayList<String> array2 = new ArrayList<String>();

                        array2 = mapper.readValue(root.get("entities").get("hashtags").toString(), ArrayList.class);
                        if (!array2.isEmpty()) {
                            JsonNode rooter;
                            for (int i = 0; i < array2.size(); i++) {
                                LOGGER.debug("HASHTAGASSSSS" + array2.toArray()[i].toString());
                                // rooter= mapper.readValue( array2.toArray()[i].toString(), JsonNode.class);
                                String[] firstPart = array2.toArray()[i].toString().split("=");
                                String[] hasj = firstPart[1].split(",");

                                LOGGER.debug("Emiting tweet with hashtag: ------------- " + hasj[0]);
                                collector.emit(new Values(timestamp, tweet_lang, hasj[0]));
                            }

                        }
                    }

                } else {
                    LOGGER.debug("tweet timestamp and/ or hashtag was null");
                }
            } else {
                LOGGER.debug("Ignoring tweets from other languages");
            }
        } catch (IOException ex) {
            LOGGER.error("IO error while filtering tweets", ex);
            LOGGER.trace(null, ex);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "lang", "hashtag"));
    }

    public Map<String, Object> getComponenetConfiguration() {
        return null;
    }
}
