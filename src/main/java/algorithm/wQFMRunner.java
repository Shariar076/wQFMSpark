package algorithm;

import config.Properties;
import wqfm.algo.FMRunner;
import wqfm.ds.CustomDSPerLevel;
import wqfm.ds.InitialTable;
import wqfm.ds.Quartet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class wQFMRunner implements Serializable {
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
    public InitialTable setLegacyInitialTable(List<String> quartetsList, CustomDSPerLevel customDS){
        InitialTable initialTable = new InitialTable();
        for(String quartet: quartetsList){
            Quartet quartet1 = new Quartet(quartet);
            initialTable.addToListOfQuartets(quartet1);
            int idx_qrt_in_table_1 = initialTable.sizeTable() - 1; //size - 1 is the last index
            customDS.quartet_indices_list_unsorted.add(idx_qrt_in_table_1);
        }
        return initialTable;
    }

    //Temp
    // public void performInitialBipartition(Dataset<SerializedQuartet> quartetsTable, int level){
    //     CustomDSPerLevel customDS_this_level = new CustomDSPerLevel();
    //     InitialTable legacyInitialTable = this.setLegacyInitialTable(quartetsTable.collectAsList(), customDS_this_level);
    //
    //     if (level == 0) { //only do this during level 0 [at the START]
    //         customDS_this_level.setInitialTableReference(legacyInitialTable); //change reference of initial table.
    //     }
    //     customDS_this_level.sortQuartetIndicesMap(); //sort the quartet-index map for initial-bipartition-computation [NOT set of quartets]
    //     customDS_this_level.fillRelevantQuartetsMap(); //fill-up the relevant quartets per taxa map
    //     if (level == 0) { //only do it for the initial step, other levels will be passed as parameters
    //         customDS_this_level.fillUpTaxaList(); //fill-up the taxa list
    //         System.out.println("Total Num-Taxa = " + customDS_this_level.taxa_list_int.size());
    //     }
    //
    //     /////////////////// TERMINATING CONDITIONS \\\\\\\\\\\\\\\\\\\\\\\\
    //     // |P| <= 3 OR |Q|.isEmpty() ... return star over taxa list{P}
    //     if ((customDS_this_level.taxa_list_int.size() <= 3)
    //             || (customDS_this_level.quartet_indices_list_unsorted.isEmpty())) {
    //         String starTree = TreeHandler.getStarTree(customDS_this_level.taxa_list_int); //depth-one tree
    //         System.out.println(starTree);
    //         // return starTree;
    //     }
    //
    //     level++; // For dummy node finding.
    //     customDS_this_level.level = level; //for debugging issues.
    //
    //     InitialBipartition initialBip = new InitialBipartition();
    //     Map<Integer, Integer> mapInitialBipartition = initialBip.getInitialBipartitionMap(customDS_this_level);
    //     System.out.println("initial bipartition map: "+ mapInitialBipartition);
    //     if (legacy.configs.Properties.DEBUG_MODE_PRINTING_GAINS_BIPARTITIONS) {
    //         System.out.println("L 84. FMComputer. Printing initialBipartition.");
    //         IOHandler.printPartition(mapInitialBipartition, DefaultValues.LEFT_PARTITION, DefaultValues.RIGHT_PARTITION, InitialTable.map_of_int_vs_str_tax_list);
    //     }
    //
    //     Bipartition8Values initialBip_8_vals = new Bipartition8Values();
    //     //**************************************************
    //     initialBip_8_vals.compute8ValuesUsingAllQuartets_this_level(customDS_this_level, mapInitialBipartition);
    //
    // }

    // public void doBipartition8ValuesCalculation(Dataset<SerializedQuartet> quartetsTable,Map<Integer, Integer> mapInitialBipartition, int level){
    //     CustomDSPerLevel customDS_this_level = new CustomDSPerLevel();
    //     InitialTable legacyInitialTable = this.setLegacyInitialTable(quartetsTable.collectAsList(), customDS_this_level);
    //
    //     if (level == 0) { //only do this during level 0 [at the START]
    //         customDS_this_level.setInitialTableReference(legacyInitialTable); //change reference of initial table.
    //     }
    //     customDS_this_level.sortQuartetIndicesMap(); //sort the quartet-index map for initial-bipartition-computation [NOT set of quartets]
    //     customDS_this_level.fillRelevantQuartetsMap(); //fill-up the relevant quartets per taxa map
    //     if (level == 0) { //only do it for the initial step, other levels will be passed as parameters
    //         customDS_this_level.fillUpTaxaList(); //fill-up the taxa list
    //         System.out.println("Total Num-Taxa = " + customDS_this_level.taxa_list_int.size());
    //     }
    //
    //     /////////////////// TERMINATING CONDITIONS \\\\\\\\\\\\\\\\\\\\\\\\
    //     // |P| <= 3 OR |Q|.isEmpty() ... return star over taxa list{P}
    //     if ((customDS_this_level.taxa_list_int.size() <= 3)
    //             || (customDS_this_level.quartet_indices_list_unsorted.isEmpty())) {
    //         String starTree = TreeHandler.getStarTree(customDS_this_level.taxa_list_int); //depth-one tree
    //         System.out.println(starTree);
    //         // return starTree;
    //     }
    //
    //     level++; // For dummy node finding.
    //     customDS_this_level.level = level;
    //
    //     Bipartition8Values initialBip_8_vals = new Bipartition8Values();
    //     //**************************************************
    //     initialBip_8_vals.compute8ValuesUsingAllQuartets_this_level(customDS_this_level, mapInitialBipartition);
    // }
    public String runDevideNConquer(List<String> quartetsList){
        System.out.println("Partition Quartets Count: "+ quartetsList.stream().count());
        FMRunner runner = new FMRunner();
        CustomDSPerLevel customDS = new CustomDSPerLevel();
        InitialTable initialTable = this.setLegacyInitialTable(quartetsList, customDS);
        // runner.readFileAndPopulateInitialTables(INPUT_FILE_NAME, customDS, initialTable);
        // System.out.println("Reading from file <" + INPUT_FILE_NAME + "> done."
        //         + "\nInitial-Num-Quartets = " + initialTable.sizeTable());
        // System.out.println("Running with partition score " + WeightedPartitionScores.GET_PARTITION_SCORE_PRINT());
        int level = 0;
        customDS.level = level; //for debugging issues.

        System.out.println(InitialTable.TAXA_COUNTER);

        System.out.println(InitialTable.map_of_str_vs_int_tax_list);
        System.out.println(InitialTable.map_of_int_vs_str_tax_list);

        String final_tree = runner.recursiveDivideAndConquer(customDS, level, initialTable); //customDS will have (P, Q, Q_relevant etc) all the params needed.
        System.out.println("\n\n[L 49.] Distributer: final tree return");

//        System.out.println(final_tree);
        String final_tree_decoded = wqfm.utils.IOHandler.getFinalTreeFromMap(final_tree, InitialTable.map_of_int_vs_str_tax_list);
        System.out.println(final_tree_decoded);
        wqfm.utils.IOHandler.writeToFile(final_tree_decoded, Properties.OUTPUT_FILE_NAME);

        return final_tree_decoded;
    }
}
