package main;

import algorithm.Distributer;
import phylonet.cmdline.tool.InferStCmdLineTool;
import phylonet.coalescent.MDCInference_DP;
import phylonet.tree.io.ParseException;
import phylonet.tree.model.sti.STITree;
import properties.ConfigValues;
import properties.DefaultConfigs;
import util.WQGenerator;

import java.io.IOException;

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
        if (ConfigValues.INPUT_MODE.equals("gene-trees")) {
            System.out.println("Input file consists of gene trees ... generating weighted quartets to file: " + DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT);
            // IOHandler.generateWeightedQuartets(ConfigValues.INPUT_FILE_NAME, ConfigValues.OUTPUT_FILE_NAME);
            WQGenerator.generateWQ(ConfigValues.INPUT_FILE_NAME, DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT);
            System.out.println("Generation of weighted quartets completed.");
            // then switch to input file name as default weighted quartets name.
            ConfigValues.INPUT_FILE_NAME = DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT;
        }

        String treeOutput = App.runwQFMSpark(ConfigValues.INPUT_FILE_NAME, ConfigValues.OUTPUT_FILE_NAME); // run wQFM
    }

    public void runCliTools(){
        new InferStCmdLineTool().printUsage(System.out);
        MDCInference_DP.main(new String[]{"-i", "input/37_taxon_all_gt.tre", "-o", "input/MDCInference_DP_ST.tre"});
        try {
            STITree tree = new STITree("(GAL:0,((ORN:0,(MAC:0,MON:0):5):17,(((CHO:0,DAS:0):1,(ECH:0,(PRO:0,LOX:0):3):3):21,(((ERI:0,SOR:0):15,((EQU:0,(FEL:0,CAN:0):2):15,((MYO:0,PTE:0):4,(VIC:0,(SUS:0,(BOS:0,TUR:0):8):14):3):26):25):4,((TUP:0,((ORY:0,OCH:0):3,((CAV:0,SPE:0):20,(DIP:0,(RAT:0,MUS:0):1):15):16):15):27,((TAR:0,(OTO:0,MIC:0):3):23,(CAL:0,(NEW:0,(PON:0,(GOR:0,(HOM:0,PAN:0):10):3):4):15):14):11):17):23):21):0):0;");
            System.out.println(tree.getNodeCount());
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
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
