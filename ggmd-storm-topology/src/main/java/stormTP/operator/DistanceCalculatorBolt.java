package stormTP.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.pojo.FilteredTortoise;
import stormTP.pojo.RaceData;
import stormTP.pojo.Tortoise;
import stormTP.pojo.TortoiseDistance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class DistanceCalculatorBolt implements IRichBolt {
    private static final long serialVersionUID = 4262369370788107343L;
    private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
    private static final ObjectMapper mapper = new ObjectMapper();

    private OutputCollector collector;

    public DistanceCalculatorBolt() {

    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
     */
    public void execute(Tuple t) {
        try {
            String n = (String) t.getValueByField("tortoises");

            List<Tortoise> tortoises =
                    mapper.readValue(n, new TypeReference<List<Tortoise>>() {});

            final List<TortoiseDistance> tortoiseDistances = new ArrayList<>();

            for (final Tortoise tr : tortoises)
            {
                final int nbCellsParcourus = tr.getTour() * tr.getMaxcel() + tr.getCellule();
                tortoiseDistances.add(new TortoiseDistance(tr.getId(), tr.getTop(), tr.getTotal(), tr.getMaxcel(), nbCellsParcourus));
            }

            final String outputJson = mapper.writeValueAsString(tortoiseDistances);
            logger.info("DistanceCalculatorBolt: outputJson = " + outputJson);
            collector.emit(new Values(outputJson));

        } catch (JsonProcessingException e) {
            logger.info("JSON parse error: " + e.getMessage());
        }

    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("tortoisesWithDistances"));
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
