package config;

import org.apache.spark.sql.SparkSession;

public class Properties {
    // ------------------------------------------------------------------------------------------------------------ //
    public static final String INPUT_FILE_NAME = "input/gtree_11tax_est_5genes_R1.tre"; //DefaultValues.INPUT_FILE_NAME_WQRTS_DEFAULT; // "input_files/weighted_quartets_avian_biological_dataset";
    public static final String OUTPUT_FILE_NAME = "output/output-gt.tree";
    public static final String HDFS_PATH = "hdfs://localhost:9000/user/himel/";
    public static SparkSession SPARK = null;
}

