package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.DistanceCalculatorBolt;
import stormTP.operator.Exit3Bolt;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.ParserBolt;

/**
 *
 * @author lumineau
 * Topologie test permettant d'écouter le Master Input
 *
 */
public class TopologyT3 {

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

        builder.setBolt("distanceCalculator", new DistanceCalculatorBolt(), nbExecutors).shuffleGrouping("parser");

        builder.setBolt("giveRankBolt", new GiveRankBolt(), nbExecutors).shuffleGrouping("distanceCalculator");

        builder.setBolt("exit", new Exit3Bolt(portOUTPUT), nbExecutors).shuffleGrouping("giveRankBolt");

        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT3", config, builder.createTopology());
    }


}