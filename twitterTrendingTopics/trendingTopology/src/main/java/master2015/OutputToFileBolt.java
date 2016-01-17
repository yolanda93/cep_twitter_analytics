package master2015;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * OutputToFileBolt
 * Created on Dec 22, 2015
 * @author Yolanda de la Hoz Simon <yolanda93h@gmail.com>
 */
public class OutputToFileBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final String ID = "53826071E";
    String[] langs;
    String file_path;

    public OutputToFileBolt(String[] languages, String folder) throws IOException {
        langs = languages;
        file_path = folder;
    }

    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {

    }

    /**
     * Method to write in files the result. File format: lang_ID.txt
     * @param tuple (1) lang, (2) lower limit of the next window, (3) first hashtag (4) first count value, (5) second hashtag, (6) second count value,(7) third hashtag, (8) third count value  
     */
    public void execute(Tuple tuple) {
        if (tuple.size() > 6) {
            File file = new File(file_path + "/" + tuple.getValue(0) + "_" + ID + ".log");
            String content = tuple.getValue(1).toString() + "," + tuple.getValue(0) + "," + tuple.getValue(2).toString() + ","
                    + tuple.getValue(3).toString() + "," + tuple.getValue(4).toString() + ","
                    + tuple.getValue(5).toString() + "," + tuple.getValue(6).toString() + ","
                    + tuple.getValue(7).toString();
            BufferedWriter bw = null;
            try {

                bw = new BufferedWriter(new FileWriter(file, true));

                bw.append(content);
                bw.newLine();
                bw.close();
            } catch (FileNotFoundException ex) {
                System.out.println(ex.getMessage());
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }

           // System.out.println("Done");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
