package wqfm.configs;

public class Config {
    public static int REROOT_MODE = DefaultValues.REROOT_USING_JAR;
    public static double SMALLEPSILON = 0; //if cumulative gain of iteration < this_num then stop

    public static double STEP_SIZE_BINNING = 0.01; //always used 0.01 for experiments (default)
    public static double THRESHOLD_BINNING = 0.9; // use 0.9 [default]
    public static boolean SET_RIGHT_TO_1 = false; //false: dual-bin (default), true: right will be set to 1.
    public static int PARTITION_SCORE_MODE = DefaultValues.PARTITION_SCORE_FULL_DYNAMIC; //0->[s]-[v], 1->[s]-0.5[v], 2->[s]-[v]-[d], 3->3[s]-2[v]
    public static boolean BIN_LIMIT_LEVEL_1 = false; // by default: false (bin on all levels); true -> only bin level 1
    public static double CUT_OFF_LIMIT_BINNING = 0.1; // use 0.1 [default]
    public static boolean NORMALIZE_DUMMY_QUARTETS = true; // true -> divide by count (use mean), false -> simply sum

    public static int MAX_ITERATIONS_LIMIT = 1000000; //can we keep it as another stopping-criterion ? [100k]
    public static boolean DEBUG_MODE_PRINTING_GAINS_BIPARTITIONS = false;

//    public static int BIPARTITION_MODE = DefaultConfigs.BIPARTITION_GREEDY;
//    public static boolean DEBUG_MODE_TESTING = true; // true -> while running from netbeans, false -> run from cmd


    public static String INPUT_FILE_NAME = "input/gtree_11tax_est_5genes_R1.tre"; //DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT; // "input_files/weighted_quartets_avian_biological_dataset";
    public static String OUTPUT_FILE_NAME = "output/output-gt.tree";
}

