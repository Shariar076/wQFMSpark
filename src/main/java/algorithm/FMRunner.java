package algorithm;

import bipartition.Bipartition8Values;
import bipartition.InitialBipartition;
import config.Config;
import config.DefaultValues;
import mapper.StringToQuartetMapper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import structure.InitialTable;
import structure.Quartet;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;


public class FMRunner {

    public static String runFunctions(String inputFileName, String outputFileName) {
        readFileAndPopulateInitialTable(inputFileName);

//         System.out.println("Reading from file <" + INPUT_FILE_NAME + "> done."
//                 + "\nInitial-Num-Quartets = " + initialTable.sizeTable());
//         System.out.println("Running with partition score " + WeightedPartitionScores.GET_PARTITION_SCORE_PRINT());
        int level = 0;
//         customDS.level = level; //for debugging issues.
//
        System.out.println(InitialTable.TAXA_COUNT);
        System.out.println(InitialTable.TAXA_LIST);
        System.out.println(InitialTable.map_of_str_vs_int_tax_list);
        System.out.println(InitialTable.map_of_int_vs_str_tax_list);

        //can recursive func be static?
        String final_tree = recursiveDivideAndConquer(level); //customDS will have (P, Q, Q_relevant etc) all the params needed.
//         System.out.println("\n\n[L 49.] FMRunner: final tree return");
//
// //        System.out.println(final_tree);
//         String final_tree_decoded = IOHandler.getFinalTreeFromMap(final_tree, LegacyInitialTable.map_of_int_vs_str_tax_list);
//         System.out.println(final_tree_decoded);
//         IOHandler.writeToFile(final_tree_decoded, OUTPUT_FILE_NAME);

        String final_tree_decoded = "tree";
        return final_tree_decoded;
    }

    public static void readFileAndPopulateInitialTable(String inputFileName) {
        Dataset<Row> sortedWqDf = Config.SPARK.read().option("header", "true")
                .csv(Config.HDFS_PATH + "/" + inputFileName)
                .orderBy(desc("count"));
        InitialTable.quartetsTable = sortedWqDf.map(new StringToQuartetMapper(), Encoders.bean(Quartet.class));

        //this will enforce spark to perform the jobs accumulated so far
        //thus ensuring initialization of variables dependent on them
        System.out.println("Initial Table Size:" + InitialTable.quartetsTable.toDF().count());
    }

    public static String recursiveDivideAndConquer(int level) {
        String finalTree = "NONE";
        // LegacyBipartition legBip = new LegacyBipartition();
        // legBip.performInitialBipartition(InitialTable.quartetsTable, level);

        // Initial Bipartition
        InitialBipartition initialBipartition = new InitialBipartition(InitialTable.TAXA_LIST);
        //monotonically_increasing_id Doesn't ensure sequenciallity but ensures increasing values; good enough
        // should be #breaks=#partitions
        Map<Integer, Integer> latestPartition = InitialTable.quartetsTable
                .toDF()
                .map((MapFunction<Row, InitialBipartition>) initialBipartition::performPartitionBasedOnQuartet, Encoders.bean(InitialBipartition.class))
                .toDF()
                .withColumn("id", monotonically_increasing_id())
                .orderBy(col("id").desc()).head().getJavaMap(2);

        System.out.println("initial bipartition after performPartitionBasedOnQuartet: " + latestPartition);
        Map<Integer, Integer> initialBipartitionMap = initialBipartition.performPartitionRandomBalanced(latestPartition);
        System.out.println("initial bipartition after performPartitionRandomBalanced: " + initialBipartitionMap);

        // legBip.doBipartition8ValuesCalculation(InitialTable.quartetsTable,initialBipartitionMap, level);
        // Bipartition scores
        Bipartition8Values initialBip_8_vals = new Bipartition8Values(initialBipartitionMap);

        Encoder<Bipartition8Values> bipartition8ValuesEncoder = Encoders.bean(Bipartition8Values.class);

        initialBip_8_vals = InitialTable.quartetsTable
                .toDF()
                .map((MapFunction<Row, Bipartition8Values>) initialBip_8_vals::compute8ValuesUsingAllQuartets_this_level, Encoders.bean(Bipartition8Values.class))
                .toDF()
                .withColumn("id", monotonically_increasing_id())
                .orderBy(col("id").desc()).as(bipartition8ValuesEncoder)
                .head();
        // System.out.println(initialBip_8_vals);
        System.out.println(initialBip_8_vals.map_four_tax_seq_weights_list);
        if (Config.PARTITION_SCORE_MODE == DefaultValues.PARTITION_SCORE_FULL_DYNAMIC) {
            initialBip_8_vals.calculateDynamicScore(level, initialBip_8_vals.getMap_four_tax_seq_weights_list());
        }

        return finalTree;
    }





}
