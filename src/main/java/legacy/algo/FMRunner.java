package legacy.algo;

import legacy.bip.Bipartition8Values;
import legacy.bip.InitialBipartition;
import legacy.bip.WeightedPartitionScores;
import legacy.configs.Config;
import legacy.ds.LegacyInitialTable;
import legacy.ds.CustomDSPerLevel;
import legacy.ds.LegacyQuartet;
import legacy.utils.IOHandler;
import legacy.utils.TreeHandler;
import legacy.configs.DefaultValues;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class FMRunner {
    public static String runFunctions(String INPUT_FILE_NAME, String OUTPUT_FILE_NAME) {
        FMRunner runner = new FMRunner();
        LegacyInitialTable legacyInitialTable = new LegacyInitialTable();
        CustomDSPerLevel customDS = new CustomDSPerLevel();
        runner.readFileAndPopulateInitialTables(INPUT_FILE_NAME, customDS, legacyInitialTable);
        System.out.println("Reading from file <" + INPUT_FILE_NAME + "> done."
                + "\nInitial-Num-Quartets = " + legacyInitialTable.sizeTable());
        System.out.println("Running with partition score " + WeightedPartitionScores.GET_PARTITION_SCORE_PRINT());
        int level = 0;
        customDS.level = level; //for debugging issues.

        System.out.println(LegacyInitialTable.TAXA_COUNTER);

        System.out.println(LegacyInitialTable.map_of_str_vs_int_tax_list);
        System.out.println(LegacyInitialTable.map_of_int_vs_str_tax_list);

        String final_tree = runner.recursiveDivideAndConquer(customDS, level, legacyInitialTable); //customDS will have (P, Q, Q_relevant etc) all the params needed.
        System.out.println("\n\n[L 49.] FMRunner: final tree return");

//        System.out.println(final_tree);
        String final_tree_decoded = IOHandler.getFinalTreeFromMap(final_tree, LegacyInitialTable.map_of_int_vs_str_tax_list);
        System.out.println(final_tree_decoded);
        IOHandler.writeToFile(final_tree_decoded, OUTPUT_FILE_NAME);

        return final_tree_decoded;
    }

    // ------>>>> Main RECURSIVE function ....
    private String recursiveDivideAndConquer(CustomDSPerLevel customDS_this_level, int level, LegacyInitialTable legacyInitialTable) {
        /*So that when customDS is passed subsequently, automatic sorting will be done. No need to do it somewhere else*/
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
            return starTree;
        }

        level++; // For dummy node finding.
        customDS_this_level.level = level; //for debugging issues.

        InitialBipartition initialBip = new InitialBipartition();
        Map<Integer, Integer> mapInitialBipartition = initialBip.getInitialBipartitionMap(customDS_this_level);
        System.out.println("initial bipartition map: "+ mapInitialBipartition);
        if (Config.DEBUG_MODE_PRINTING_GAINS_BIPARTITIONS) {
            System.out.println("L 84. FMComputer. Printing initialBipartition.");
            IOHandler.printPartition(mapInitialBipartition, DefaultValues.LEFT_PARTITION, DefaultValues.RIGHT_PARTITION, LegacyInitialTable.map_of_int_vs_str_tax_list);
        }

        Bipartition8Values initialBip_8_vals = new Bipartition8Values();
        //**************************************************
        initialBip_8_vals.compute8ValuesUsingAllQuartets_this_level(customDS_this_level, mapInitialBipartition);
        //**************************************************
        System.out.println(WeightedPartitionScores.GET_PARTITION_SCORE_PRINT() + " LEVEL: " + level + ", ALPHA: " + WeightedPartitionScores.ALPHA_PARTITION_SCORE + ", BETA: " + WeightedPartitionScores.BETA_PARTITION_SCORE);

        FMComputer fmComputerObject = new FMComputer(customDS_this_level, mapInitialBipartition, initialBip_8_vals, level);
        FMResultObject fmResultObject = fmComputerObject.run_FM_Algorithm_Whole();

        CustomDSPerLevel customDS_left = fmResultObject.customDS_left_partition;
        CustomDSPerLevel customDS_right = fmResultObject.customDS_right_partition;


        /////////////////// Beginning of Recursion \\\\\\\\\\\\\\\\\\\\\\\\\\\
        int dummyTaxon = fmResultObject.dummyTaxonThisLevel;
        String left_tree_unrooted = recursiveDivideAndConquer(customDS_left, level, legacyInitialTable);
        String right_tree_unrooted = recursiveDivideAndConquer(customDS_right, level, legacyInitialTable);
        String merged_tree = TreeHandler.mergeUnrootedTrees(left_tree_unrooted, right_tree_unrooted, String.valueOf(dummyTaxon));
        return merged_tree;
    }

    // https://stackoverflow.com/questions/6100712/simple-way-to-count-character-occurrences-in-a-string/23906674
    private int countChars_in_String(String str, char c) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == c) {
                count++;
            }
        }
        return count;
    }

    //------------------Initial Bipartition --------------------
    private void populatePerInputLine(CustomDSPerLevel customDS, LegacyInitialTable legacyInitialTable, String line, int line_cnt) {
        // Check if STAR is present
        // ((1,2,49,57)); 6     is a STAR
        // ((0,1),(10,9)); 343  is normal // should have 3 right brackets and 3 left brackets

        int cnt_left_brackets = countChars_in_String(line, '(');
        int cnt_right_brackets = countChars_in_String(line, ')');

        if ((cnt_left_brackets != cnt_right_brackets) || (cnt_left_brackets != 3) || (cnt_right_brackets != 3)) {
            System.out.println("\n\n****** Found STAR, line num " + line_cnt + " :->" + line);
            System.out.println("\nCan't handle polytomy for now. Exiting System.\n\n");
            System.exit(-1);
        }

        // No issues with STAR.
        LegacyQuartet legacyQuartet = new LegacyQuartet(line);
        legacyInitialTable.addToListOfQuartets(legacyQuartet); //add to initial-quartets-single-list
        int idx_qrt_in_table_1 = legacyInitialTable.sizeTable() - 1; //size - 1 is the last index
        customDS.quartet_indices_list_unsorted.add(idx_qrt_in_table_1);
    }

    //BufferedReader is used here since BufferedReader is faster than Scanner.readLine
    public void readFileAndPopulateInitialTables(String inputFileName, CustomDSPerLevel customDS, LegacyInitialTable legacyInitialTable) {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(inputFileName);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
            try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                String line;
                int line_cnt = 1;
                while ((line = bufferedReader.readLine()) != null && !line.isEmpty()) {
                    populatePerInputLine(customDS, legacyInitialTable, line, line_cnt);
                    line_cnt++;
                }
            }
        } catch (IOException ex) {
            System.out.println("ERROR READING FILE <" + inputFileName + ">. EXITING SYSTEM");
            System.exit(-1);

        } finally {
            try {
                fileInputStream.close();
            } catch (IOException ex) {
                System.out.println("ERROR IN CLOSING fileInputStream while reading file. Exiting");
                System.exit(-1);
            }
        }
        //FeatureComputer.Compute_Feature(legacyInitialTable.get_QuartetList());

    }
}
