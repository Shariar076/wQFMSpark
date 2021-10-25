package wqfm.configs;

public interface DefaultValues {
    // Use this for logical biparititoning
    int LEFT_PARTITION = -1; //-1 : left
    int UNASSIGNED_PARTITION = 0;
    int RIGHT_PARTITION = 1; //+1 : right

    int UNKNOWN = 75;
    int SATISFIED = 73;
    int VIOLATED = 72;
    int DEFERRED = 71;
    int BLANK = 70;

    int REROOT_USING_JAR = 41;
    int REROOT_USING_PYTHON = 42;
    int REROOT_USING_PERL = 43;


    int LEFT_SISTER_1_IDX = 0;
    int LEFT_SISTER_2_IDX = 1;
    int RIGHT_SISTER_1_IDX = 2;
    int RIGHT_SISTER_2_IDX = 3;


    int PARTITION_SCORE_MODE_0 = 0;
    int PARTITION_SCORE_MODE_1 = 1;
    int PARTITION_SCORE_MODE_2 = 2;
    int PARTITION_SCORE_MODE_3 = 3;
    int PARTITION_SCORE_MODE_4 = 4;
    int PARTITION_SCORE_MODE_5 = 5;
    int PARTITION_SCORE_MODE_6 = 6;
    int PARTITION_SCORE_MODE_7 = 7;
    int PARITTION_SCORE_COMMAND_LINE = 8;
    int PARTITION_SCORE_FULL_DYNAMIC = 9;
    double ALPHA_DEFAULT_VAL = 1.0;
    double BETA_DEFAULT_VAL = 1.0;
    int BIPARTITION_GREEDY = 1;
//    int BIPARTITION_EXTREME = 2;
//    int BIPARTITION_RANDOM = 3;
//    int ANNOTATIONS_LEVEL0_NONE = 0;


    String INPUT_MODE_WEIGHTED_QUARTETS = "weighted-quartets";
    String INPUT_MODE_DEFAULT = INPUT_MODE_WEIGHTED_QUARTETS;
    String OFF = "off";
    String NULL = "NULL";
    String INPUT_FILE_NAME_WQRTS_DEFAULT =  "input/input-weighted-quartets.csv";

}
