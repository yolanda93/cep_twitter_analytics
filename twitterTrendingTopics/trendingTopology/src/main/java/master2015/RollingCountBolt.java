package master2015;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

  

public class RollingCountBolt extends BaseRichBolt {

  private static final long serialVersionUID = 5537727428628598519L;
  private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
  private static final int NUM_WINDOW_CHUNKS = 5;
  private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
  private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
  private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
      "Actual window length is %d seconds when it should be %d seconds"
          + " (you can safely ignore this warning during the startup phase)";

  private final Map<String,SlidingWindowCounter<String>>  counter; // Key =  lang / Value = SlidingWindowCounter
  private final int windowLengthInSeconds;
  private final int advance_window;
  private OutputCollector collector;

  public RollingCountBolt() {
    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
    this.windowLengthInSeconds = windowLengthInSeconds;
    this.advance_window = emitFrequencyInSeconds;
    this.counter = new HashMap<String,SlidingWindowCounter<String>>();
  }
  
  private void createNewSlidingWindow(String lang, long timestamp) {
   SlidingWindowCounter<String> lang_window;
    if ( counter.get(lang) == null) {
      lang_window = new SlidingWindowCounter<String>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
        this.advance_window),calculateInitialInterval(timestamp));
      counter.put(lang, lang_window);
    }
  }
  
  private int calculateInitialInterval(long timestamp) {
    System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + this.advance_window / timestamp);
    return (int) (timestamp / this.advance_window);
  }

  private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
    return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;

  }
  
   private int getCurrentLowerLimit(String lang) {
       
    return ((counter.get(lang).windowIntervalCounter)*advance_window); 
  }
  

  private boolean isAdvanceTimeReached(String lang,long timestamp) {
      System.out.println("LOWER LIMIT: "+ (getCurrentLowerLimit(lang)));
      System.out.println("TIMESTAMP 2: "+ timestamp);
      System.out.println("TO REACH THE LIMIT: "+ (timestamp-getCurrentLowerLimit(lang)));
      return timestamp-getCurrentLowerLimit(lang)>=advance_window;
  }
  
   private long milisecondToSeconds(Long timestamp) {
    System.out.println("TIMESTAMP 1: "+ timestamp);
    return (long) (timestamp / 1000); 
  }
  
  @Override
    public void execute(Tuple tuple) {
        if (tuple.size() > 2) {
            String lang = (String) tuple.getValue(1);
            long timestamp = milisecondToSeconds(Long.valueOf(tuple.getString(0)));
            System.out.println("------------> new TIMESTAMP: "+ timestamp);
            createNewSlidingWindow(lang,timestamp);
            if (!isAdvanceTimeReached(lang,timestamp)) {
                countObjAndAck(tuple);
            } else {
                LOG.debug("----------->Advance time reached");
                emitCurrentWindowCounts(lang,timestamp);
                countObjAndAck(tuple);
            }
        }
    }

  private void emitCurrentWindowCounts(String lang, long timestamp) { 
    int n_intervals_advance = calculateInitialInterval(timestamp-getCurrentLowerLimit(lang));
    System.out.println("Advance intervals" + n_intervals_advance);
    Map<String, Long> counts = counter.get(lang).getCountsThenAdvanceWindow(n_intervals_advance);
    emit(counts,lang);
  }
  
  
    private void getTop3(Map<String, Long> counts) {
      List<Long> c;
      c = new ArrayList<Long>(counts.values());
        Collections.sort(c);
        LOG.debug(" TOP 3----------------------------------------------------------!!!!!!!!!");
        if(counts.size()>=3){
        for (int i = 2; i >= 0; i--) { // The top 3 is emitted.
            System.out.println(i + " rank is " + c.get(i));
        }
        }
       
    }
    
   public LinkedHashMap sortHashMapByValuesD(HashMap passedMap) {
   List mapKeys = new ArrayList(passedMap.keySet());
   List mapValues = new ArrayList(passedMap.values());
   Collections.sort(mapValues);
   Collections.sort(mapKeys);

   LinkedHashMap sortedMap = new LinkedHashMap();

   Iterator valueIt = mapValues.iterator();
   while (valueIt.hasNext()) {
       Object val = valueIt.next();
       Iterator keyIt = mapKeys.iterator();

       while (keyIt.hasNext()) {
           Object key = keyIt.next();
           String comp1 = passedMap.get(key).toString();
           String comp2 = val.toString();

           if (comp1.equals(comp2)){
               passedMap.remove(key);
               mapKeys.remove(key);
               sortedMap.put((String)key, (Double)val);
               break;
           }

       }

   }
   return sortedMap;
}
 
private void emit(Map<String, Long> counts, String lang) {
        Set<Entry<String, Long>> set = counts.entrySet();
        List<Entry<String, Long>> list = new ArrayList<Entry<String, Long>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });
        for (Map.Entry<String, Long> entry : list) {
            System.out.println(entry.getKey() + " ==== " + entry.getValue());

            LOG.debug("coun:" + entry.getValue() + "actualLowerLimit:" + getCurrentLowerLimit(lang));
            collector.emit(new Values(entry.getKey(), entry.getValue(), getCurrentLowerLimit(lang)));
        }
    }


  private void countObjAndAck(Tuple tuple) {
    String obj = tuple.getValue(2).toString();
    counter.get(tuple.getValue(1).toString()).incrementCount(obj);
    //collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, advance_window);
    return conf;
  }
  
}
