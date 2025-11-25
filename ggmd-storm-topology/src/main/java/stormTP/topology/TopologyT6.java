package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import stormTP.operator.DistanceCalculatorBolt;
import stormTP.operator.Exit6Bolt;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.ParserBolt;
import stormTP.operator.RankEvolutionBolt;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author lumineau
 * Topologie test permettant d'écouter le Master Input
 *
 */
public class TopologyT6 {

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

        builder.setBolt("distanceCalculatorBolt", new DistanceCalculatorBolt(), nbExecutors).shuffleGrouping("parser");

        builder.setBolt("giveRankBolt", new GiveRankBolt(), nbExecutors).shuffleGrouping("distanceCalculatorBolt");

        builder.setBolt("myTortoise", new MyTortoiseBolt(), nbExecutors).shuffleGrouping("giveRankBolt");

        builder.setBolt("rankEvolution", new RankEvolutionBolt()
                .withWindow(
                        new BaseWindowedBolt.Duration(30, TimeUnit.SECONDS),
                        new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS)
                ), nbExecutors).shuffleGrouping("myTortoise");

        builder.setBolt("exit", new Exit6Bolt(portOUTPUT), nbExecutors).shuffleGrouping("rankEvolution");

        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT6", config, builder.createTopology());
    }


}