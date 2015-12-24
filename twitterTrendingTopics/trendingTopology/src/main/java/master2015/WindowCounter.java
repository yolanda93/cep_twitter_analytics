package master2015;

import java.io.Serializable;
import java.util.Map;

/**
*
* @author Yolanda de la Hoz Simon - 53826071E
*/
@SuppressWarnings("hiding")
public final class WindowCounter<String> implements Serializable {

  private static final long serialVersionUID = -2645063988768785810L;

  private SlideCounter<String> hashtag_counter;
  private int head_slide;
  private int tail_slide;
  private int window_length_slides;
  public int window_slide_counter;
  public int initialIntervalWindow;

  public WindowCounter(int window_length_slides, int initial_interval_window) {
    this.window_length_slides = window_length_slides;
    this.hashtag_counter = new SlideCounter<String>(this.window_length_slides);
    this.window_slide_counter=initial_interval_window;
    this.head_slide = 0;
    this.tail_slide = slideAfter(head_slide);
  }

  public void incrementCount(String obj) {
    hashtag_counter.incrementCount(obj, head_slide);
  }

  private void advanceHead() {
    head_slide = tail_slide;
    tail_slide = slideAfter(tail_slide);
  }

  private int slideAfter(int slide) {
    return (slide + 1) % window_length_slides;
  }
  
  public Map<String, Long> getCountsSlideWindow(int n_intervals_advance) {
	    Map<String, Long> counts = hashtag_counter.getCounts();
	    hashtag_counter.wipeZeros();
	    hashtag_counter.wipeSlot(tail_slide);
	    advanceHead();
	    window_slide_counter = window_slide_counter + n_intervals_advance;
	    return counts;
	  }


}
