package properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ConfigValues {
    public static final String TRIPLETS_SODA_PATH = "./triplets.soda2103";
    public static final String INPUT_FILE_NAME = "input/gtree_11tax_est_5genes_R1.tre"; //DefaultConfigs.INPUT_FILE_NAME_WQRTS_DEFAULT; // "input_files/weighted_quartets_avian_biological_dataset";
    public static final String OUTPUT_FILE_NAME = "output/output-gt.tree";
    public static final String HDFS_PATH = "hdfs://localhost:9000/user/himel/";
    public static final int TAXA_PER_PARTITION = 8;

    public static SparkSession SPARK =SparkSession.builder()
            .appName("wQFMSpark")
            // .master("local")
            .master("spark://doer-ThinkPad-T460s:7077")
            // .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
            .getOrCreate();;
}

