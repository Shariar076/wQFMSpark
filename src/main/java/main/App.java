package main;

import algorithm.Distributer;
import properties.Configs;
import properties.DefaultValues;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import util.WQGenerator;
import wqfm.utils.TreeHandler;

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

    public static void initializeAndRun(SparkSession spark){
        Configs.SPARK = spark;

        System.out.println("Input file consists of gene trees ... generating weighted quartets to file: " + DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT);
        // IOHandler.generateWeightedQuartets(Configs.INPUT_FILE_NAME, Configs.OUTPUT_FILE_NAME);
        WQGenerator.generateWQ(Configs.INPUT_FILE_NAME, DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT);
        System.out.println("Generation of weighted quartets completed.");
        // then switch to input file name as default weighted quartets name.

        String treeOutput = App.runwQFMSpark(DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT, Configs.OUTPUT_FILE_NAME); // run wQFM
    }
    public static String addBracketsAndSemiColon(String s) {
        return "(" + s + ");";
    }
    private static String removeBracketsAndSemiColon(String tree, String outGroup) {
        tree = tree.replace(";", ""); // remove semi-colon
        tree = tree.substring(1, tree.length() - 1); // remove first and last brackets
        // tree = tree.replace(outGroup, ""); //remove outGroup Node
        // tree = tree.substring(1); //From left, so remove first comma
        return tree;
    }

    public static String treeMerger(){
        String tree1 = "((5,(10,8)),(4,(1,2)));";
        String tree2 = "((6,(9,(7,8))),(3,11));";
        String rootedTreeLeft = TreeHandler.rerootTree_JAR(tree1, "8");
        String rootedTreeRight = TreeHandler.rerootTree_JAR(tree2, "8");
        System.out.println("rootedTreeLeft: "+ rootedTreeLeft);
        System.out.println("rootedTreeRight: "+ rootedTreeRight);
        String leftTree_outgroupRemoved = removeBracketsAndSemiColon(rootedTreeLeft, "8");
        String rightTree_outgroupRemoved = removeBracketsAndSemiColon(rootedTreeRight, "8"); //CHECK if from both sides outgroup are in left

        String mergedTree = addBracketsAndSemiColon(leftTree_outgroupRemoved + "," + rightTree_outgroupRemoved);
        System.out.println("mergedTree: "+ mergedTree);
        return mergedTree;
    }
    public static void main(String[] args) {
        // wQFMRun.minimalCall();
        // treeMerger();
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
