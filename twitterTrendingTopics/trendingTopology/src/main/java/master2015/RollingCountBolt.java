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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individual
 * 
 * ly for each object within a window.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 */
public class RollingCountBolt extends BaseRichBolt {

  private static final long serialVersionUID = 5537727428628598519L;
  private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
  private static final int NUM_WINDOW_CHUNKS = 5;
  private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
  private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
  private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
      "Actual window length is %d seconds when it should be %d seconds"
          + " (you can safely ignore this warning during the startup phase)";

  private final Map<String,SlidingWindowCounter<Object>>  counter; // Key =  lang / Value = SlidingWindowCounter
  private final int windowLengthInSeconds;
  private final int advance_window;
  private OutputCollector collector;

  public RollingCountBolt() {
    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
    this.windowLengthInSeconds = windowLengthInSeconds;
    this.advance_window = emitFrequencyInSeconds;
    this.counter = new HashMap<String,SlidingWindowCounter<Object>>();
  }
  
  private void createNewSlidingWindow(String lang, long timestamp) {
   SlidingWindowCounter<Object> lang_window;
    if ( counter.get(lang) == null) {
      lang_window = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
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
    Map<Object, Long> counts = counter.get(lang).getCountsThenAdvanceWindow(n_intervals_advance);
    emit(counts,lang);
  }
  
    private void getTop3(Map<Object, Long> counts) {
      List<Long> c;
      c = new ArrayList<Long>(counts.values());
        Collections.sort(c);
        LOG.debug(" TOP 3----------------------------------------------------------!!!!!!!!!");
       
        for (int i = 3; i >= 0; i--) { // The top 3 is emitted.
            System.out.println(i + " rank is " + c.get(i));
        }
       
    }
 
  private void emit(Map<Object, Long> counts,String lang) {
      getTop3(counts);
    for (Entry<Object, Long> entry : counts.entrySet()) {
      Object obj = entry.getKey();
      Long count = entry.getValue();
      
      LOG.debug("coun:" + count + "actualLowerLimit:" + getCurrentLowerLimit(lang));
      collector.emit(new Values(obj, count, getCurrentLowerLimit(lang)));
    }
  }

  private void countObjAndAck(Tuple tuple) {
    Object obj = tuple.getValue(2);
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
