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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;

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
            String timestamp;
            String tweet_lang;
            String[] hashtag;
            if (root.get("lang") != null && isValidLang(root.get("lang").textValue())) {
                if (root.get("timestamp_ms") != null && root.get("entities").get("hashtags") != null) {
                    timestamp = root.get("timestamp_ms").textValue();
                    tweet_lang = root.get("lang").textValue();
                    String array = root.get("entities").get("hashtags").toString();
                    if (array.length() > 0) {
                        ArrayList<String> hashtags_list;
                        hashtags_list = mapper.readValue(root.get("entities").get("hashtags").toString(), ArrayList.class);
                        if (!hashtags_list.isEmpty()) {
                            for (int i = 0; i < hashtags_list.size(); i++) {
                                String[] firstPart = hashtags_list.toArray()[i].toString().split("=");
                                hashtag = firstPart[1].split(",");
                                LOGGER.debug("--------------> Emiting tweet with hashtag: " + hashtag[0]);
                                LOGGER.debug("--------------> Emiting tweet with lang: " + tweet_lang);
                                collector.emit(new Values(timestamp, tweet_lang, hashtag[0]));
                            }
                        }
                    } else {
                        LOGGER.debug("hashtag was null");
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
