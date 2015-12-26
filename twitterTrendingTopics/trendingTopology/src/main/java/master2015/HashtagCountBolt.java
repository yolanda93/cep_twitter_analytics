package master2015;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

  
/**
* HashtagCountBolt
* Created on Dec 22, 2015
* @author Yolanda de la Hoz Simon <yolanda93h@gmail.com>
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
  
  /**
  * Method to create the time interval window to count hashtags in a given language.
  * @param lang hashtag language 
  * @param timestamp hashtag timestamp used to calculate the current time interval window. It will be the initial interval window.      
  */
  private void createNewTimeIntervalWindow(String lang, long timestamp) {
   WindowCounter<String> lang_window;
    if ( counter.get(lang) == null) {
      lang_window = new WindowCounter<String>(getNumberSlides(this.window_size_seconds,
        this.advance_window),calculateInitialInterval(timestamp));
      counter.put(lang, lang_window);
    }
  }
  
  /**
  * Method to calculate the initial time interval
  * @param timestamp hashtag timestamp used to calculate the current time interval window. It will be the initial interval window.      
  */
  private int calculateInitialInterval(long timestamp) {
    System.out.println("The initial interval is: " + this.advance_window / timestamp);
    return (int) (timestamp / this.advance_window);
  }

  /**
  * Method to divide in slides the current time interval window
  * @param window_size_seconds maximum size of the window time interval
  * @param advance_seconds the size of the slide
  */
  private int getNumberSlides(int window_size_seconds, int advance_seconds) {
    return window_size_seconds / advance_seconds;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }
  
  /**
  * Method to get the lower limit of the next window
  * @param lang language as identifier
  */
   private int getCurrentLowerLimit(String lang) {
    return ((counter.get(lang).window_slide_counter)*advance_window); 
  }
  
 /**
 * Method to check is reached the maximum of the slide
 * @param lang language as identifier
 * @param timestamp timestamp of the current hashtag
 */
  private boolean isAdvanceTimeReached(String lang,long timestamp) {
      System.out.println("LOWER LIMIT: "+ (getCurrentLowerLimit(lang)));
      System.out.println("TIMESTAMP: "+ timestamp);
      System.out.println("TO REACH THE LIMIT: "+ (timestamp-getCurrentLowerLimit(lang)));
      return timestamp-getCurrentLowerLimit(lang)>=advance_window;
  }
  
  /**
   * Method to pass from miliseconds to seconds
   * @param timestamp language as identifier
   */
   private long milisecondToSeconds(Long timestamp) {
    System.out.println("TIMESTAMP 1: "+ timestamp);
    return (long) (timestamp / 1000); 
  }
   
   @Override
    public void execute(Tuple tuple) {
        if (tuple.size() > 2) {
            String lang = (String) tuple.getValue(1);
            long timestamp = milisecondToSeconds(Long.valueOf(tuple.getString(0)));
            System.out.println("------------> NEW TIMESTAMP: "+ timestamp);
            createNewTimeIntervalWindow(lang,timestamp);
            if (!isAdvanceTimeReached(lang,timestamp)) {
                addCounterHashtag(tuple);
            } else {
                System.out.println("----------->Advance time reached");
                emitCurrentCounts(lang,timestamp);
                addCounterHashtag(tuple);
            }
        }
    }

  /**
  * Method that advance the time to reach the limit to set the next window and get the current counts.
  * The window slide counter is increased to reach the next current lower limit given by the current timestamp
  * @param lang the lang of the current hashtag
  * @param timestamp timestamp of the current hashtag
  */
  private void emitCurrentCounts(String lang, long timestamp) { 
    int n_intervals_advance = calculateInitialInterval(timestamp-getCurrentLowerLimit(lang));
    System.out.println("Advance intervals" + n_intervals_advance);
    Map<String, Long> counts = counter.get(lang).getCountsWindowAndAdvance(n_intervals_advance);
    emit(counts,lang);
  }
  
  /**
  * Method that emits the current counts ordered
  * @param hashtag_counts map with the counts of each hashtag
  * @param lang language of the current hashtag
  */
  private void emit(Map<String, Long> hashtag_counts, String lang) {
        Set<Entry<String, Long>> set = hashtag_counts.entrySet();
        List<Entry<String, Long>> list = new ArrayList<Entry<String, Long>>(set);
        System.out.println("Emitted list of window:" + getCurrentLowerLimit(lang) + "languague:" + lang );
        collector.emit(new Values(list, getCurrentLowerLimit(lang),lang));
  }

  /**
  * Method that increments the current hashtag counter
  * @param tuple tuple with hashtag
  */
  private void addCounterHashtag(Tuple tuple) {
    String hashtag = tuple.getValue(2).toString();
    counter.get(tuple.getValue(1).toString()).incrementCount(hashtag);
  }

  /**
   * Method to declare the output fields
   * @param declarer. Receives as tuple: (1) an array list with the counts of each hashtag, (2) lower limit of the next window and (3) the language.
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("hashtag_counts","currentLowerLimit", "lang"));
  }

  
}
