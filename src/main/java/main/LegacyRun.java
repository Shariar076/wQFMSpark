package main;

import legacy.algo.FMRunner;
import legacy.configs.Config;
import legacy.configs.DefaultValues;
import legacy.utils.IOHandler;
import util.TreeHandler;

public class LegacyRun {
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

    public static String runwQFM(String inputFilename, String outputFileName) {
        // Call wQFM runner here. ?
        System.out.println("================= **** ========== Running WQFM ============== **** ====================");

        long time_1 = System.currentTimeMillis(); //calculate starting time

        LegacyRun.testIfRerootWorks(); // test to check if phylonet jar is attached correctly.

        String tree = FMRunner.runFunctions(inputFilename, outputFileName); //main functions for wQFM

        long time_del = System.currentTimeMillis() - time_1;
        long minutes = (time_del / 1000) / 60;
        long seconds = (time_del / 1000) % 60;
        System.out.format("\nTime taken = %d ms ==> %d minutes and %d seconds.\n", time_del, minutes, seconds);
        System.out.println("================= **** ======================== **** ====================");

        return tree;
    }

    public static void minimalCall() {
        Config.INPUT_FILE_NAME = "./input/gtree_11tax_est_5genes_R1.tre";
        Config.OUTPUT_FILE_NAME = "./output/output-gt.tree";

        System.out.println("Input file consists of gene trees ... generating weighted quartets to file: " + DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT);
        IOHandler.generateWeightedQuartets(Config.INPUT_FILE_NAME, DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT);
        // remove file generated temporarily
        // Helper.removeFile(DefaultValues.TEMP_WEIGHTED_QUARTETS_FILE_TO_REMOVE)
        System.out.println("Generating weighted quartets completed.");
        // then switch to input file name as default weighted quartets name.
        Config.INPUT_FILE_NAME = DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT;
        String treeOutput = LegacyRun.runwQFM(Config.INPUT_FILE_NAME, Config.OUTPUT_FILE_NAME); // run wQFM
    }

}
