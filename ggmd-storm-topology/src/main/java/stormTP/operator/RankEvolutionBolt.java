package stormTP.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import stormTP.pojo.FilteredTortoise;
import stormTP.pojo.TortoiseEvolution;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class RankEvolutionBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {
    private static final long serialVersionUID = 4262387370788107343L;
    private static Logger logger = Logger.getLogger("RankEvolutionBoltLogger");
    private static final ObjectMapper mapper = new ObjectMapper();
    private OutputCollector collector;

    private KeyValueState<String, Integer> state;
    private int position;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, Integer> state) {
        this.state = state;
        position = state.get("sum", 0);
        System.out.println("initState with state [" + state + "] current pos [" + position + "]");
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        if (inputWindow.get().isEmpty() || inputWindow.get().size() < 2) {
            return;
        }

        try {
            final List<Tuple> tuples = inputWindow.get();
            final Tuple tupleAtFirstRecord = tuples.get(0);
            final Tuple tupleAtLastRecord = tuples.get(tuples.size() - 1);

            final FilteredTortoise myTortoiseAtBeginning =
                    mapper.readValue(tupleAtFirstRecord.getValueByField("myTortoise").toString(), FilteredTortoise.class);
            final FilteredTortoise myTortoiseAtEnd = mapper.readValue(tupleAtLastRecord.getValueByField("myTortoise").toString(), FilteredTortoise.class);

            final String rankEvolution = computeEvolution(myTortoiseAtBeginning.getRang(), myTortoiseAtEnd.getRang());

            final long timestamp = System.currentTimeMillis();

            final TortoiseEvolution te = new TortoiseEvolution(myTortoiseAtBeginning.getId(), myTortoiseAtBeginning.getNom(), timestamp, rankEvolution);

            final String outputJson = mapper.writeValueAsString(te);
            logger.info("TortoiseEvolution output [" + outputJson + "]");
            collector.emit(inputWindow.get(), new Values(outputJson));

        } catch (JsonProcessingException e) {
            logger.info("RankEvolution, erreur lors de la récupération des informations sur la tortue");
            throw new RuntimeException(e);
        }
    }

    private String computeEvolution(String oldRank, String newRank) {
        final int oldestRank = Integer.parseInt(oldRank.split(" ")[0]);
        final int newestRank = Integer.parseInt(newRank.split(" ")[0]);

        if (newestRank < oldestRank) {
            return "En progression";
        } else if (newestRank > oldestRank) {
            return "En régression";
        } else {
            return "Constant";
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}






