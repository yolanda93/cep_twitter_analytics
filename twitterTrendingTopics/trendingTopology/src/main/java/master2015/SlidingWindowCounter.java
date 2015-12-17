package master2015;

import java.io.Serializable;
import java.util.Map;

public final class SlidingWindowCounter<String> implements Serializable {

  private static final long serialVersionUID = -2645063988768785810L;

  private SlotBasedCounter<String> objCounter;
  private int headSlot;
  private int tailSlot;
  private int windowLengthInSlots;
  public int windowIntervalCounter;
  public int initialIntervalWindow;

  public SlidingWindowCounter(int windowLengthInSlots, int initial_interval_window) {
    if (windowLengthInSlots < 2) {
      throw new IllegalArgumentException(
          "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
    }
    this.windowLengthInSlots = windowLengthInSlots;
    this.objCounter = new SlotBasedCounter<String>(this.windowLengthInSlots);
    this.windowIntervalCounter=initial_interval_window;
    this.headSlot = 0;
    this.tailSlot = slotAfter(headSlot);
  }

  public void incrementCount(String obj) {
    objCounter.incrementCount(obj, headSlot);
  }

  public Map<String, Long> getCountsThenAdvanceWindow(int n_intervals_advance) {
    Map<String, Long> counts = objCounter.getCounts();
    objCounter.wipeZeros();
    objCounter.wipeSlot(tailSlot);
    advanceHead();
    windowIntervalCounter = windowIntervalCounter+n_intervals_advance;
    return counts;
  }

  private void advanceHead() {
    headSlot = tailSlot;
    tailSlot = slotAfter(tailSlot);
  }

  private int slotAfter(int slot) {
    return (slot + 1) % windowLengthInSlots;
  }

}
