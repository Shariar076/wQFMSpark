package algorithm;

import bipartion.InitialBipartition;
import config.Config;
import mapper.QuartetStringMapper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import structure.InitialTable;
import structure.Quartet;

import java.util.Map;

import static org.apache.spark.sql.functions.desc;

public class FMRunner {

    public static String runFunctions(String inputFileName, String outputFileName) {
        readFileAndPopulateInitialTable(inputFileName);

//         System.out.println("Reading from file <" + INPUT_FILE_NAME + "> done."
//                 + "\nInitial-Num-Quartets = " + initialTable.sizeTable());
//         System.out.println("Running with partition score " + WeightedPartitionScores.GET_PARTITION_SCORE_PRINT());
//         int level = 0;
//         customDS.level = level; //for debugging issues.
//
        System.out.println(InitialTable.TAXA_COUNT);
        System.out.println(InitialTable.TAXA_LIST);
        System.out.println(InitialTable.map_of_str_vs_int_tax_list);
        System.out.println(InitialTable.map_of_int_vs_str_tax_list);

        //can recursive func be static?
        String final_tree = recursiveDivideAndConquer(); //customDS will have (P, Q, Q_relevant etc) all the params needed.
//         System.out.println("\n\n[L 49.] FMRunner: final tree return");
//
// //        System.out.println(final_tree);
//         String final_tree_decoded = IOHandler.getFinalTreeFromMap(final_tree, InitialTable.map_of_int_vs_str_tax_list);
//         System.out.println(final_tree_decoded);
//         IOHandler.writeToFile(final_tree_decoded, OUTPUT_FILE_NAME);

        String final_tree_decoded = "tree";
        return final_tree_decoded;
    }

    public static void readFileAndPopulateInitialTable(String inputFileName) {
        Dataset<Row> sortedWqDf = Config.SPARK.read().option("header","true")
                .csv(Config.HDFS_PATH+"/"+inputFileName)
                .orderBy(desc("count"));
        InitialTable.quartetsTable = sortedWqDf.map(new QuartetStringMapper(), Encoders.bean(Quartet.class));

        //this will enforce spark to perform the jobs accumulated so far
        //thus ensuring initialization of variables dependent on them
        System.out.println("Initial Table Size:" + InitialTable.quartetsTable.toDF().count());
    }

    public static String recursiveDivideAndConquer(){
        String finalTree =  "NONE";
        // Initial Bipartition
        InitialBipartition initialBipartition = new InitialBipartition(InitialTable.TAXA_LIST, InitialTable.quartetsTable);
        Dataset<String> mapHistory = InitialTable.quartetsTable.toDF().map((MapFunction<Row, String>)
                initialBipartition::performPartitionBasedOnQuartet, Encoders.STRING());
        // mapHistory.show(false);
        Map <Integer, Integer> initialBipMap  = initialBipartition.performPartitionRandomBalanced();
        System.out.println(initialBipMap);



        return finalTree;
    }


}
