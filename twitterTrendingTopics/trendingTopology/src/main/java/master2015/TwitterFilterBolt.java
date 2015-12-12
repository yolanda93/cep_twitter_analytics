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

public class TwitterFilterBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =Logger.getLogger(TwitterFilterBolt.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    String [] lang;
    public TwitterFilterBolt(String[] languages) {
        this.lang=languages;
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("timestamp","lang", "text"));
    }
    
    public boolean isValidLang(String tweet_lang){
      for( int i = 0; i <= lang.length - 1; i++)
      {
        if(lang[i].equals(tweet_lang))
           return true; 
      }
      return false;
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        LOGGER.debug("Filttering incoming tweets");
        String json = input.getString(0);
        try
        {
            JsonNode root = mapper.readValue(json, JsonNode.class);
            long timestamp;
            String tweet_lang;
            String text;
            if (root.get("lang") != null && isValidLang(root.get("lang").textValue()))
            {
                if (root.get("timestamp") != null && root.get("entities").get("hashtags").get("text") != null)
                {
                    timestamp=root.get("timestamp").longValue();
                    tweet_lang = root.get("lang").textValue();
                    text = root.get("entities").get("hashtags").get("text").textValue();
                    collector.emit(new Values(timestamp,tweet_lang, text));
                }
                else
                    LOGGER.debug("tweet timestamp and/ or text was null");
            }
            else
                LOGGER.debug("Ignoring tweets from other languages");
        }
        catch (IOException ex)
        {
            LOGGER.error("IO error while filtering tweets", ex);
            LOGGER.trace(null, ex);
        }
    }

    public Map<String, Object> getComponenetConfiguration() { return null; }
}