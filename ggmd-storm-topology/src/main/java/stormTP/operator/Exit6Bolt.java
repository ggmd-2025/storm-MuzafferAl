package stormTP.operator;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.stream.StreamEmiter;

import java.util.Map;
import java.util.logging.Logger;


public class Exit6Bolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107342L;
    private static Logger logger = Logger.getLogger("Exit6Bolt");
    private static ObjectMapper mapper = new ObjectMapper();
    //private static Logger logger = Logger.getLogger("ExitBolt");
    private OutputCollector collector;
    int port = -1;
    StreamEmiter semit = null;

    public Exit6Bolt(int port) {
        this.port = port;
    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
     */
    public void execute(Tuple t) {
        String n = (String) t.getValueByField("json");
        logger.info("Exit6Bolt : output : " + n);
        this.semit.send(n);
        collector.ack(t);

        return;
    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("json"));
    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#getComponentConfiguration()
     */
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IBasicBolt#cleanup()
     */
    public void cleanup() {

    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
        this.semit = new StreamEmiter(this.port);
        this.collector = collector;
    }
}