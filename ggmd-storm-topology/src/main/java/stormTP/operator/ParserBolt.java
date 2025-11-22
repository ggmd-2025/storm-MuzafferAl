package stormTP.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.pojo.RaceData;

import java.util.Map;
import java.util.logging.Logger;

public class ParserBolt implements IRichBolt {
    private static final long serialVersionUID = 4262369370788107343L;
    private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
    private static final ObjectMapper mapper = new ObjectMapper();

    private OutputCollector collector;

    public ParserBolt() {

    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
     */
    public void execute(Tuple t) {
        try {
            String n = t.getValueByField("json").toString();
            final RaceData raceData = mapper.readValue(n, RaceData.class);

            final String outputJson = mapper.writeValueAsString(raceData.getRunners());
            logger.info("ParserBolt: outputJson = " + outputJson);
            collector.emit(new Values(outputJson));

        } catch (JsonProcessingException e) {
            logger.info("JSON parse error: " + e.getMessage());
        }

    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("tortoises"));
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
        this.collector = collector;
    }
}
