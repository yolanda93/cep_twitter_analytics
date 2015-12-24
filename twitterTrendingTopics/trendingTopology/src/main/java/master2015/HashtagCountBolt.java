package master2015;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

  
/**
*
* @author Yolanda de la Hoz Simon - 53826071E
*/
public class HashtagCountBolt extends BaseRichBolt {

  private static final long serialVersionUID = 1L;
  private final Map<String,WindowCounter<String>>  counter; // Key =  lang / Value = SlidingWindowCounter
  private final int advance_window;
  private final int window_size_seconds;
  private OutputCollector collector;

  public HashtagCountBolt(int window_length_seconds, int emit_frequency_seconds) {
    this.window_size_seconds = window_length_seconds;
    this.advance_window = emit_frequency_seconds;
    this.counter = new HashMap<String,WindowCounter<String>>();
  }
  
  private void createNewSlidingWindow(String lang, long timestamp) {
   WindowCounter<String> lang_window;
    if ( counter.get(lang) == null) {
      lang_window = new WindowCounter<String>(get_num_slides_window(this.window_size_seconds,
        this.advance_window),calculateInitialInterval(timestamp));
      counter.put(lang, lang_window);
    }
  }
  
  private int calculateInitialInterval(long timestamp) {
    System.out.println("The initial interval is: " + this.advance_window / timestamp);
    return (int) (timestamp / this.advance_window);
  }

  private int get_num_slides_window(int window_size_seconds, int advance_seconds) {
    return window_size_seconds / advance_seconds;
  }

  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }
  
   private int getCurrentLowerLimit(String lang) {
    return ((counter.get(lang).window_slide_counter)*advance_window); 
  }
  

  private boolean isAdvanceTimeReached(String lang,long timestamp) {
      System.out.println("LOWER LIMIT: "+ (getCurrentLowerLimit(lang)));
      System.out.println("TIMESTAMP: "+ timestamp);
      System.out.println("TO REACH THE LIMIT: "+ (timestamp-getCurrentLowerLimit(lang)));
      return timestamp-getCurrentLowerLimit(lang)>=advance_window;
  }
  
   private long milisecondToSeconds(Long timestamp) {
    System.out.println("TIMESTAMP 1: "+ timestamp);
    return (long) (timestamp / 1000); 
  }

    public void execute(Tuple tuple) {
        if (tuple.size() > 2) {
            String lang = (String) tuple.getValue(1);
            long timestamp = milisecondToSeconds(Long.valueOf(tuple.getString(0)));
            System.out.println("------------> NEW TIMESTAMP: "+ timestamp);
            createNewSlidingWindow(lang,timestamp);
            if (!isAdvanceTimeReached(lang,timestamp)) {
                addCounterHashtag(tuple);
            } else {
                System.out.println("----------->Advance time reached");
                emitCurrentCounts(lang,timestamp);
                addCounterHashtag(tuple);
            }
        }
    }

  private void emitCurrentCounts(String lang, long timestamp) { 
    int n_intervals_advance = calculateInitialInterval(timestamp-getCurrentLowerLimit(lang));
    System.out.println("Advance intervals" + n_intervals_advance);
    Map<String, Long> counts = counter.get(lang).getCountsSlideWindow(n_intervals_advance);
    emit(counts,lang);
  }
  
 
    private void emit(Map<String, Long> counts, String lang) {
        Set<Entry<String, Long>> set = counts.entrySet();
        List<Entry<String, Long>> list = new ArrayList<Entry<String, Long>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });
        int top3 = 0;
        String []hashtags=new String[3];
        Long []count=new Long[3];
        for (Map.Entry<String, Long> entry : list) {
            hashtags[top3]=entry.getKey();
            count[top3]=entry.getValue();
            System.out.println(entry.getKey() + " ==== " + entry.getValue());
            top3++;
            if (top3 == 3) {
            	long real_time_milisec=(long)getCurrentLowerLimit(lang)*1000;
                collector.emit(new Values(lang,real_time_milisec,hashtags[0],count[0],hashtags[1],count[1],hashtags[2],count[2]));
                break;
            }         
        }
    }


  private void addCounterHashtag(Tuple tuple) {
    String hashtag = tuple.getValue(2).toString();
    counter.get(tuple.getValue(1).toString()).incrementCount(hashtag);
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("lang","currentLowerLimit", "hashtag1", "count1","hashtag2", "count2","hashtag3", "count3"));
  }

  
}
