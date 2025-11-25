package stormTP.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.pojo.TortoiseDistance;
import stormTP.pojo.TortoiseRank;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ComputeBonusBolt implements IRichBolt {
    private static final long serialVersionUID = 4262369370788107343L;
    private static Logger logger = Logger.getLogger("ComputeBonusBoltLogger");
    private static final ObjectMapper mapper = new ObjectMapper();

    private OutputCollector collector;

    public ComputeBonusBolt() {

    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
     */
    public void execute(Tuple t) {
        try {
            String n = (String) t.getValueByField("tortoisesWithDistances");

            List<TortoiseDistance> tortoises =
                    mapper.readValue(n, new TypeReference<List<TortoiseDistance>>() {
                    });

            tortoises.sort(Comparator.comparingInt(TortoiseDistance::nbCellsParcourus));

            final List<TortoiseRank> tortoisesRanked = new ArrayList<>();

            for (int i = 0; i < tortoises.size(); i++) {

                final TortoiseDistance tr = tortoises.get(i);

                String tortoiseRank = String.valueOf(i + 1);
                boolean exAequo = tortoises.stream()
                        .anyMatch(other ->
                                other.nbCellsParcourus() == tr.nbCellsParcourus() && other.id() != tr.id()
                        );

                if (exAequo){
                    tortoiseRank += " ex";
                }

                final TortoiseRank tortoiseRanked = new TortoiseRank(tr.id(), tr.top(), tortoiseRank, tr.total(), tr.maxcel());
                tortoisesRanked.add(tortoiseRanked);
            }

            final String outputJson = mapper.writeValueAsString(tortoisesRanked);
            logger.info("GiveRankBolt: outputJson = " + outputJson);
            collector.emit(new Values(outputJson));

        } catch (JsonProcessingException e) {
            logger.info("JSON parse error: " + e.getMessage());
        }

    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("tortoisesRanked"));
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
