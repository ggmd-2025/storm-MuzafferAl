package stormTP.operator.test;

import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;



public class TestStatefulWindowBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {
	private static final long serialVersionUID = 4262379330788107343L;
    private  KeyValueState<String, Integer> state;
    private  int sum;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, Integer> state) {
        this.state = state;
        sum = state.get("sum", 0);
        System.out.println("initState with state [" + state + "] current sum [" + sum + "]");
    }

    @Override
    public void execute(TupleWindow inputWindow) {
    	
    	int cpt = 0;
		
        for (Tuple t : inputWindow.get()) {
        	cpt++;
    		
    	}
        state.put("sum", cpt);
        
        JsonObjectBuilder r = Json.createObjectBuilder();
        r.add("test", "statelessWithWindow");
        r.add("nbNewTuples", cpt);
		r.add("totalNumberOfTuples", cpt);
        JsonObject row = r.build();
	    
        collector.emit(inputWindow.get(),new Values(row.toString()));
        
    }

    private String determineEvolution(int oldRank, int newRank) {
        if (newRank < oldRank) {
            return "En progression";
        } else if (newRank > oldRank) {
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

