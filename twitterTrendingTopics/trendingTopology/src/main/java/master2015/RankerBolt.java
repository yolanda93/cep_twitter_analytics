package master2015;

import backtype.storm.task.OutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * OutputToFileBolt
 * Created on Dec 22, 2015
 * @author Yolanda de la Hoz Simon <yolanda93h@gmail.com>
 */
public class RankerBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    public RankerBolt() throws IOException {
    }

    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {
     this.collector = oc;
    }
    
    
    private void emitTop3(List<Entry<String, Long>> list, String lang,int lowerlimit){
    	int top3 = 0;
        String []hashtags=new String[3];
        Long []count=new Long[3];
        for (Map.Entry<String, Long> entry : list) {
            hashtags[top3]=entry.getKey();
            count[top3]=entry.getValue();
            System.out.println(lang + entry.getKey() + " ==== " + entry.getValue());
            top3++;
            if (top3 == 3) {
            	long real_time_milisec=(long)lowerlimit*1000;
                collector.emit(new Values(lang,real_time_milisec,hashtags[0],count[0],hashtags[1],count[1],hashtags[2],count[2]));
                break;
            }         
        }
    }

    /**
     * Method to order a given array list to get the top 3 twitter topics
     * @param tuple. Receives as tuple: (1) an array list with the counts of each hashtag, (2) lower limit of the next window and (3) the language.
     */
    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
        List<Entry<String, Long>> list;
    	if(tuple.size()>2){
         list= (List<Entry<String, Long>>) tuple.getValue(0);  	
    	 Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
             public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                 return (o2.getValue()).compareTo(o1.getValue());
             }
         });
    	 emitTop3(list,tuple.getValue(2).toString(),(int)tuple.getValue(1));
    	}
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    	ofd.declare(new Fields("lang","currentLowerLimit", "hashtag1", "count1","hashtag2", "count2","hashtag3", "count3"));
    }

}