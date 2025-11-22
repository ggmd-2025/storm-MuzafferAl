package stormTP.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.pojo.RaceData;
import stormTP.pojo.Tortoise;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


/**
 * Sample of stateless operator
 *
 * @author lumineau
 *
 */
public class MyTortoiseBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107343L;

    private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
    private static ObjectMapper mapper = new ObjectMapper();

    private static final int MY_TORTOISE_ID = 2;
    private static final String MY_TORTOISE_NAME = "Mad";

    private OutputCollector collector;


    public MyTortoiseBolt() {

    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
     */
    public void execute(Tuple t) {

        try {
            String n = t.getValueByField("json").toString();
            final RaceData raceData = mapper.readValue(n, RaceData.class);

            final List<Tortoise> tortoises = raceData.getRunners();

            tortoises.stream()
                    .filter(tr -> tr.getId() == MY_TORTOISE_ID)
                    .forEach(tr -> {
                        final int nbCellsParcourus = tr.getTour() * tr.getMaxcel() + tr.getCellule();
                        collector.emit(new Values(
                                tr.getId(),
                                tr.getTop(),
                                MY_TORTOISE_NAME,
                                nbCellsParcourus,
                                tr.getTotal(),
                                tr.getMaxcel()
                        ));
                    });
            collector.ack(t);
        } catch (Exception e) {
            System.err.println("Empty tuple.");
        }
    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("id", "top", "nom", "nbCellsParcourus", "total", "maxcel"));
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