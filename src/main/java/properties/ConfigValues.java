package properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import phylonet.tree.model.sti.STITree;

public class ConfigValues {
    public static final String TRIPLETS_SODA_PATH = "./triplets.soda2103";
    public static String INPUT_MODE = "gene-trees"; //wqrt, gene-trees
    public static String INPUT_FILE_NAME = "input/37_taxon_all_gt.tre"; //"input/37_taxa.wqrts";
    public static String OUTPUT_FILE_NAME = "output/output-gt.tree";
    public static final String HDFS_PATH = "hdfs://localhost:9000/user/himel/";
    public static final int TAXA_PER_PARTITION = 20;

    public static SparkSession SPARK = getSPARK();

    public static SparkSession getSPARK() {
        // spark = SparkSession.builder()
        // .appName("wQFMSpark")
        // // .master("local")
        // .master("spark://doer-ThinkPad-T460s:7077")
        // // .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        // .getOrCreate();
        // spark.sparkContext().setLogLevel("WARN");

        SparkConf conf = new SparkConf().setAppName("wQFMSpark")
                // .setJars(new String[]{System.getProperty("user.dir") + "/target/wQFMSpark-1.0-SNAPSHOT-jar-with-dependencies.jar"})
                .setMaster("local")
                // .setMaster("spark://doer-ThinkPad-T460s:7077")
                ;
        // conf.registerKryoClasses(new Class<?>[]{ STITree.class});
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        return SparkSession.builder().config(jsc.getConf()).getOrCreate();
    }
}

