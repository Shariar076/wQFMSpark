package properties;

import org.apache.spark.sql.SparkSession;

public class Configs {
    // ------------------------------------------------------------------------------------------------------------ //
    public static final String INPUT_FILE_NAME = "input/gtree_15tax_100g_100b_R1.tre"; //DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT; // "input_files/weighted_quartets_avian_biological_dataset";
    public static final String OUTPUT_FILE_NAME = "output/output-gt.tree";
    public static final String HDFS_PATH = "hdfs://localhost:9000/user/himel/";
    public static SparkSession SPARK = null;
}

