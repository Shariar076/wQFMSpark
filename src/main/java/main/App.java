package main;

import algorithm.Distributer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import properties.ConfigValues;
import properties.DefaultConfigs;
import util.WQGenerator;

/**
 * Hello world!
 */
public class App {


    public static String runwQFMSpark(String inputFilename, String outputFileName) {
        // Call wQFM runner here. ?
        System.out.println("================= **** ========== Running wQFM on Spark ============== **** ====================");

        long time_1 = System.currentTimeMillis(); //calculate starting time


        String tree = Distributer.runFunctions(inputFilename, outputFileName); //main functions for wQFM

        long time_del = System.currentTimeMillis() - time_1;
        long minutes = (time_del / 1000) / 60;
        long seconds = (time_del / 1000) % 60;
        System.out.format("\nTime taken = %d ms ==> %d minutes and %d seconds.\n", time_del, minutes, seconds);
        System.out.println("================= **** ======================== **** ====================");

        return tree;
    }

    public static void initializeAndRun(){
        // ConfigValues.SPARK = spark;
        ConfigValues.SPARK.sparkContext().setLogLevel("WARN");
        System.out.println("Input file consists of gene trees ... generating weighted quartets to file: " + DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT);
        // IOHandler.generateWeightedQuartets(ConfigValues.INPUT_FILE_NAME, ConfigValues.OUTPUT_FILE_NAME);
        WQGenerator.generateWQ(ConfigValues.INPUT_FILE_NAME, DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT);
        System.out.println("Generation of weighted quartets completed.");
        // then switch to input file name as default weighted quartets name.

        String treeOutput = App.runwQFMSpark(DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT, ConfigValues.OUTPUT_FILE_NAME); // run wQFM
    }

    public static void main(String[] args) {
        // wQFMRun.minimalCall();
        // SparkConf conf = new SparkConf().setAppName("test DF")
        //         // .setJars(new String[]{System.getProperty("user.dir") + "/target/wQFMSpark-1.0-SNAPSHOT-jar-with-dependencies.jar"})
        //         // .setMaster("local")
        //         .setMaster("spark://doer-ThinkPad-T460s:7077")
        //         ;
        // JavaSparkContext jsc = new JavaSparkContext(conf);
        // jsc.setLogLevel("WARN");
        // SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        // SQLContext sqlContext = new SQLContext(jsc); //deprecated
        initializeAndRun();
    }
}
