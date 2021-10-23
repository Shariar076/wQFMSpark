package algorithm;

import config.Config;
import legacy.bip.InitialBipartition;
import legacy.utils.TreeHandler;
import mapper.StringToQuartetMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import structure.InitialTable;
import structure.Quartet;

import legacy.ds.CustomDSPerLevel;
import legacy.ds.LegacyInitialTable;
import legacy.ds.LegacyQuartet;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;

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

    //Temp
    public static Map<Integer, Integer> stringToMap(String mapString){
        Map<Integer, Integer> retMap = new HashMap<Integer, Integer>();
        String[] pairs = mapString.replaceAll("\\{|\\}|\\s+","").split(",");
        for (int i=0;i<pairs.length;i++) {
            String pair = pairs[i];
            String[] keyValue = pair.split("=");
            retMap.put(Integer.valueOf(keyValue[0]), Integer.valueOf(keyValue[1]));
        }
        return retMap;
    }
    //Temp
    public LegacyInitialTable setLegacyInitialTable(Dataset<Quartet> quartetsTable, CustomDSPerLevel customDS){
        LegacyInitialTable legacyInitialTable = new LegacyInitialTable();
        for(Quartet quartet: quartetsTable.collectAsList()){
            LegacyQuartet quartet1 = new LegacyQuartet(quartet.toString());
            legacyInitialTable.addToListOfQuartets(quartet1);
            int idx_qrt_in_table_1 = legacyInitialTable.sizeTable() - 1; //size - 1 is the last index
            customDS.quartet_indices_list_unsorted.add(idx_qrt_in_table_1);
        }
        return legacyInitialTable;
    }
    //Temp
    public void performLegacyRun(Dataset<Quartet> quartetsTable, int level){
        CustomDSPerLevel customDS_this_level = new CustomDSPerLevel();
        LegacyInitialTable legacyInitialTable = this.setLegacyInitialTable(quartetsTable, customDS_this_level);

        if (level == 0) { //only do this during level 0 [at the START]
            customDS_this_level.setInitialTableReference(legacyInitialTable); //change reference of initial table.
        }
        customDS_this_level.sortQuartetIndicesMap(); //sort the quartet-index map for initial-bipartition-computation [NOT set of quartets]
        customDS_this_level.fillRelevantQuartetsMap(); //fill-up the relevant quartets per taxa map
        if (level == 0) { //only do it for the initial step, other levels will be passed as parameters
            customDS_this_level.fillUpTaxaList(); //fill-up the taxa list
            System.out.println("Total Num-Taxa = " + customDS_this_level.taxa_list_int.size());
        }

        /////////////////// TERMINATING CONDITIONS \\\\\\\\\\\\\\\\\\\\\\\\
        // |P| <= 3 OR |Q|.isEmpty() ... return star over taxa list{P}
        if ((customDS_this_level.taxa_list_int.size() <= 3)
                || (customDS_this_level.quartet_indices_list_unsorted.isEmpty())) {
            String starTree = TreeHandler.getStarTree(customDS_this_level.taxa_list_int); //depth-one tree
            System.out.println(starTree);
            // return starTree;
        }

        level++; // For dummy node finding.
        customDS_this_level.level = level; //for debugging issues.

        InitialBipartition initialBip = new InitialBipartition();
        Map<Integer, Integer> mapInitialBipartition = initialBip.getInitialBipartitionMap(customDS_this_level);
        System.out.println("initial bipartition map: "+ mapInitialBipartition);
    }

    public static String recursiveDivideAndConquer(int level) {
        String finalTree = "NONE";
        FMRunner fmRunner = new FMRunner();

        // Initial Bipartition
        fmRunner.performLegacyRun(InitialTable.quartetsTable, level);

        // InitialBipartition initialBipartition = new InitialBipartition(LegacyInitialTable.TAXA_LIST);
        // //monotonically_increasing_id Doesn't ensure sequenciallity but ensures increasing values; good enough
        // // should be #breaks=#partitions
        // Map<Integer, Integer> latestPartition = LegacyInitialTable.quartetsTable
        //         .toDF()
        //         .map((MapFunction<Row, InitialBipartition>) initialBipartition::performPartitionBasedOnQuartet, Encoders.bean(InitialBipartition.class))
        //         .toDF()
        //         .withColumn("id", monotonically_increasing_id())
        //         .orderBy(col("id").desc()).head().getJavaMap(2);
        //
        // System.out.println("initial bipartition after performPartitionBasedOnQuartet: " + latestPartition);
        // Map<Integer, Integer> initialBipartitionMap = initialBipartition.performPartitionRandomBalanced(latestPartition);
        // System.out.println("initial bipartition after performPartitionRandomBalanced: " + initialBipartitionMap);
        // //Bipartition scores
        // Bipartition8Values initialBip_8_vals = new Bipartition8Values(initialBipartitionMap);
        //
        // Encoder<Bipartition8Values> bipartition8ValuesEncoder = Encoders.bean(Bipartition8Values.class);
        //
        // initialBip_8_vals = LegacyInitialTable.quartetsTable
        //         .toDF()
        //         .map((MapFunction<Row, Bipartition8Values>) initialBip_8_vals::compute8ValuesUsingAllQuartets_this_level, Encoders.bean(Bipartition8Values.class))
        //         .toDF()
        //         .withColumn("id", monotonically_increasing_id())
        //         .orderBy(col("id").desc()).as(bipartition8ValuesEncoder)
        //         .head();
        // System.out.println(initialBip_8_vals);
        // // System.out.println(latestBipartition8Values.map_four_tax_seq_weights_list);
        // initialBip_8_vals.calculateDynamicScore(level, initialBip_8_vals.getMap_four_tax_seq_weights_list());


        return finalTree;
    }


}
