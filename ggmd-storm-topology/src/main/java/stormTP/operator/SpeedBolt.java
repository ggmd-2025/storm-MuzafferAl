package stormTP.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import stormTP.pojo.Tortoise;
import stormTP.pojo.TortoiseSpeed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class SpeedBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 4262387370788107343L;
    private static Logger logger = Logger.getLogger("SpeedBoltLogger");
    private static final ObjectMapper mapper = new ObjectMapper();
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        if (inputWindow.get().isEmpty()) {
            return;
        }

        try {
            final Map<Integer, List<Tortoise>> dataByTortoises = new HashMap<>();
            for (Tuple t : inputWindow.get()) {
                String n = (String) t.getValueByField("tortoises");

                List<Tortoise> tortoises =
                        mapper.readValue(n, new TypeReference<List<Tortoise>>() {
                        });

                for (final Tortoise tr : tortoises) {
                    dataByTortoises
                            .computeIfAbsent(tr.getId(), k -> new ArrayList<>())
                            .add(tr);
                }
            }

            final List<TortoiseSpeed> output = new ArrayList<>();
            for (Map.Entry<Integer, List<Tortoise>> entry : dataByTortoises.entrySet()) {
                Integer tortoiseId = entry.getKey();
                List<Tortoise> measures = entry.getValue();

                double minTop = Double.MAX_VALUE;
                double maxTop = Double.MIN_VALUE;
                int posAtBeginning = 0;
                int postAtEnd = 0;

                for (Tortoise tr : measures) {
                    double currentTop = tr.getTop();

                    if (currentTop < minTop) {
                        minTop = currentTop;
                        posAtBeginning = tr.getCellule();
                    }
                    if (currentTop > maxTop) {
                        maxTop = currentTop;
                        postAtEnd = tr.getCellule();
                    }
                }
                output.add(new TortoiseSpeed(tortoiseId, String.valueOf(minTop) + "-" + String.valueOf(maxTop), "tortoise-" + tortoiseId,
                        (float) (postAtEnd - posAtBeginning) / measures.size()));
            }
            final String outputJson = mapper.writeValueAsString(output);
            logger.info("output : " + outputJson);
            collector.emit(inputWindow.get(), new Values(outputJson));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tortoisesSpeed"));
    }
}






