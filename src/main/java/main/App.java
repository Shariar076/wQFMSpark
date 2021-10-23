package main;

import algorithm.FMRunner;
import config.Config;
import config.DefaultValues;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import util.TreeHandler;
import util.WQGenerator;

/**
 * Hello world!
 */
public class App {
    private static void testIfRerootWorks() {
        try {
            //Test a dummy reroot function. To check if "lib" is in correct folder.
            String newickTree = "((3,(1,2)),((6,5),4));";
            String outGroupNode = "5";
            TreeHandler.rerootTree(newickTree, outGroupNode);
        } catch (Exception e) {
            System.out.println("Reroot not working, check if lib is in correct folder. Exiting.");
            System.exit(-1);
        }
    }

    public static String runwQFMSpark(String inputFilename, String outputFileName) {
        // Call wQFM runner here. ?
        System.out.println("================= **** ========== Running wQFM on Spark ============== **** ====================");

        long time_1 = System.currentTimeMillis(); //calculate starting time

        App.testIfRerootWorks(); // test to check if phylonet jar is attached correctly.

        String tree = FMRunner.runFunctions(inputFilename, outputFileName); //main functions for wQFM

        long time_del = System.currentTimeMillis() - time_1;
        long minutes = (time_del / 1000) / 60;
        long seconds = (time_del / 1000) % 60;
        System.out.format("\nTime taken = %d ms ==> %d minutes and %d seconds.\n", time_del, minutes, seconds);
        System.out.println("================= **** ======================== **** ====================");

        return tree;
    }

    public static void initializeAndRun(SparkSession spark){
        Config.SPARK = spark;

        System.out.println("Input file consists of gene trees ... generating weighted quartets to file: " + DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT);
        // IOHandler.generateWeightedQuartets(Config.INPUT_FILE_NAME, Config.OUTPUT_FILE_NAME);
        // WQGenerator.generateWQ(Config.INPUT_FILE_NAME, DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT);
        System.out.println("Generation of weighted quartets completed.");
        // then switch to input file name as default weighted quartets name.

        String treeOutput = App.runwQFMSpark(DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT, Config.OUTPUT_FILE_NAME); // run wQFM
    }
    public static void main(String[] args) {
        // LegacyRun.minimalCall();

        SparkConf conf = new SparkConf().setAppName("test DF")
                // .setJars(new String[]{System.getProperty("user.dir") + "/target/wQFMSpark-1.0-SNAPSHOT-jar-with-dependencies.jar"})
                .setMaster("local")
                // .setMaster("spark://doer-ThinkPad-T460s:7077")
                ;
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        // SQLContext sqlContext = new SQLContext(jsc); //deprecated
        initializeAndRun(spark);
    }
}
