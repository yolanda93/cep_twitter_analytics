package master2015;

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

/**
 * TwitterFilterBolt
 * Created on Dec 22, 2015
 * @author Yolanda de la Hoz Simon <yolanda93h@gmail.com>
 */
public class TwitterFilterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();
    String[] lang;

    public TwitterFilterBolt(String[] languages) {
        this.lang = languages;
    }

    /**
     * Method check if the given language is on the list of selected languages or not
     * @param tweet_lang language of the tweet to be processed
     */
    private boolean isValidLang(String tweet_lang) {
        for (int i = 0; i <= lang.length - 1; i++) {
            if (lang[i].equals(tweet_lang)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Method to filter incoming tweets
     * @param input received tweets 
     * @param collector used to emit the tweets
     */
    @SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("Filttering incoming tweets");
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
                                System.out.println("--------------> Emiting tweet with hashtag: " + hashtag[0]);
                                System.out.println("--------------> Emiting tweet with lang: " + tweet_lang);
                                collector.emit(new Values(timestamp, tweet_lang, hashtag[0]));
                            }
                        }
                    } else {
                        System.out.println("hashtag was null");
                    }
                } else {
                    System.out.println("tweet timestamp and/ or hashtag was null");
                }
            } else {
                System.out.println("Ignoring tweets from other languages");
            }
        } catch (IOException ex) {
            System.out.println("IO error while filtering tweets:" + ex.getMessage());
        }
    }
    
    /**
     * Method to declare the output fields
     * @param declarer declare the output fields
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "lang", "hashtag"));
    }

    public Map<String, Object> getComponenetConfiguration() {
        return null;
    }
}
