package algorithms;

import configs.Config;
import mappers.QuartetStringMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import structures.InitialTable;
import structures.Quartet;

import static org.apache.spark.sql.functions.desc;

public class FMRunner {

    public static String runFunctions(String inputFileName, String outputFileName) {



//         legacy.algo.FMRunner runner = new legacy.algo.FMRunner();
        InitialTable initialTable = new InitialTable();
        readFileAndPopulateInitialTables(inputFileName, initialTable);

//         System.out.println("Reading from file <" + INPUT_FILE_NAME + "> done."
//                 + "\nInitial-Num-Quartets = " + initialTable.sizeTable());
//         System.out.println("Running with partition score " + WeightedPartitionScores.GET_PARTITION_SCORE_PRINT());
//         int level = 0;
//         customDS.level = level; //for debugging issues.
//
//         System.out.println(InitialTable.TAXA_COUNTER);
//
        System.out.println(InitialTable.map_of_str_vs_int_tax_list);
        System.out.println(InitialTable.map_of_int_vs_str_tax_list);

        //can recursive func be static?
        String final_tree = recursiveDivideAndConquer(initialTable); //customDS will have (P, Q, Q_relevant etc) all the params needed.
//         System.out.println("\n\n[L 49.] FMRunner: final tree return");
//
// //        System.out.println(final_tree);
//         String final_tree_decoded = IOHandler.getFinalTreeFromMap(final_tree, InitialTable.map_of_int_vs_str_tax_list);
//         System.out.println(final_tree_decoded);
//         IOHandler.writeToFile(final_tree_decoded, OUTPUT_FILE_NAME);

        String final_tree_decoded = "tree";
        return final_tree_decoded;
    }

    public static void readFileAndPopulateInitialTables(String inputFileName, InitialTable initialTable) {
        Dataset<Row> sortedWqDf = Config.SPARK.read().option("header","true")
                .csv(Config.HDFS_PATH+"/"+inputFileName)
                .orderBy(desc("count"));
        initialTable.initializeQuartetsTable(sortedWqDf.map(new QuartetStringMapper(), Encoders.bean(Quartet.class)));
        initialTable.showQuartetsTable();
    }

    public static String recursiveDivideAndConquer(InitialTable initialTable){
        String finalTree =  "NONE";
        initialTable.showQuartetsTable();
        return finalTree;
    }


}
