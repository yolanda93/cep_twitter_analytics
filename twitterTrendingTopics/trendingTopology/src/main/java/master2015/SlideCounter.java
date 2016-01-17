package master2015;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * SlideCounter
 * Created on Dec 22, 2015
 * @author Yolanda de la Hoz Simon <yolanda93h@gmail.com>
 */
public final class SlideCounter<String> implements Serializable {

	private static final long serialVersionUID = 1L;
	private final Map<String, long[]> hashtag_counter = new HashMap<String, long[]>();
	private final int num_slides;

	public SlideCounter(int n_slides) {
		this.num_slides = n_slides;
	}

	public void incrementCount(String hashtag, int slide) {
		long[] counts = hashtag_counter.get(hashtag);
		if (counts == null) {
			counts = new long[this.num_slides];
			hashtag_counter.put(hashtag, counts);
		}
		counts[slide]++;
	}

	public long getCount(String obj, int slot) {
		long[] counts = hashtag_counter.get(obj);
		if (counts == null) {
			return 0;
		} else {
			return counts[slot];
		}
	}

	public Map<String, Long> getCounts() {
		Map<String, Long> result = new HashMap<String, Long>();
		for (String obj : hashtag_counter.keySet()) {
			result.put(obj, getTotalCount(obj));
		}
		return result;
	}

	public void wipeSlot(int slot) {
		for (String obj : hashtag_counter.keySet()) {
			resetSlotCountToZero(obj, slot);
		}
	}

	private void resetSlotCountToZero(String hashtag, int slide) {
		long[] counts = hashtag_counter.get(hashtag);
		counts[slide] = 0;
	}

	private boolean shouldBeRemovedFromCounter(String hashtag) {
		return getTotalCount(hashtag) == 0;
	}

	private long getTotalCount(String obj) {
		long[] current = hashtag_counter.get(obj);
		long total = 0;
		for (long l : current) {
			total += l;
		}
		return total;
	}

  public void wipeZeros() {
    Set<String> hashtag_to_remove = new HashSet<String>();
    for (String hashtag : hashtag_counter.keySet()) {
      if (shouldBeRemovedFromCounter(hashtag)) {
        hashtag_to_remove.add(hashtag);
      }
    }
    for (String hash : hashtag_to_remove) {
      hashtag_counter.remove(hash);
    }
  }

}