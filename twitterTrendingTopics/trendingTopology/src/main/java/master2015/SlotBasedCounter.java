package master2015;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class SlotBasedCounter<String> implements Serializable {

  private static final long serialVersionUID = 4858185737378394432L;

  private final Map<String, long[]> objToCounts = new TreeMap<String, long[]>();
  private final int numSlots;

  public SlotBasedCounter(int numSlots) {
    if (numSlots <= 0) {
      throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
    }
    this.numSlots = numSlots;
  }

  public void incrementCount(String obj, int slot) {
    long[] counts = objToCounts.get(obj);
    if (counts == null) {
      counts = new long[this.numSlots];
      objToCounts.put(obj, counts);
    }
    counts[slot]++;
  }

  public long getCount(String obj, int slot) {
    long[] counts = objToCounts.get(obj);
    if (counts == null) {
      return 0;
    }
    else {
      return counts[slot];
    }
  }

  public Map<String, Long> getCounts() {
    Map<String, Long> result = new TreeMap<String, Long>();
    for (String obj : objToCounts.keySet()) {
      result.put(obj, computeTotalCount(obj));
    }
    return result;
  }

  private long computeTotalCount(String obj) {
    long[] curr = objToCounts.get(obj);
    long total = 0;
    for (long l : curr) {
      total += l;
    }
    return total;
  }

  public void wipeSlot(int slot) {
    for (String obj : objToCounts.keySet()) {
      resetSlotCountToZero(obj, slot);
    }
  }

  private void resetSlotCountToZero(String obj, int slot) {
    long[] counts = objToCounts.get(obj);
    counts[slot] = 0;
  }

  private boolean shouldBeRemovedFromCounter(String obj) {
    return computeTotalCount(obj) == 0;
  }


  public void wipeZeros() {
    Set<String> objToBeRemoved = new HashSet<String>();
    for (String obj : objToCounts.keySet()) {
      if (shouldBeRemovedFromCounter(obj)) {
        objToBeRemoved.add(obj);
      }
    }
    for (String obj : objToBeRemoved) {
      objToCounts.remove(obj);
    }
  }

}