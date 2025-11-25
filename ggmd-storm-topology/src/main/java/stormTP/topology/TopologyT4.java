package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import stormTP.operator.Exit4Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.ParserBolt;
import stormTP.operator.SpeedBolt;

/**
 *
 * @author lumineau
 * Topologie test permettant d'écouter le Master Input
 *
 */
public class TopologyT4 {

    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        /*Création du spout*/
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);

        builder.setBolt("parser", new ParserBolt(), nbExecutors).shuffleGrouping("masterStream");

        BaseWindowedBolt speedBolt = new SpeedBolt()
                .withWindow(
                        new BaseWindowedBolt.Count(10),
                        new BaseWindowedBolt.Count(5)
                );

        builder.setBolt("speedCalculator", speedBolt, nbExecutors).shuffleGrouping("parser");

        builder.setBolt("exit", new Exit4Bolt(portOUTPUT), nbExecutors).shuffleGrouping("speedCalculator");

        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
    }


}