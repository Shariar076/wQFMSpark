package properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import phylonet.tree.model.sti.STITree;

public class ConfigValues {
    public static final String TRIPLETS_SODA_PATH = "./triplets.soda2103";
    public static String INPUT_MODE = "wqrt"; //wqrt, gene-trees
    public static String INPUT_FILE_NAME = "input/101_taxa_01.true.wqrts"; //"input/37_taxa.wqrts";
    public static String OUTPUT_FILE_NAME = "output/output-gt.tree";
    public static final String HDFS = "hdfs://ms0819.utah.cloudlab.us:9000";
    public static final String HDFS_USER = "kab076";
    public static final String HDFS_PATH = HDFS + "/user/" + HDFS_USER + "/";
    public static final int TAXA_PER_PARTITION = 100;
    public static final int NUM_WORKER = 3;

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
                // .setMaster("local")
                .setMaster("spark://node0:7077")
                ;
        // conf.registerKryoClasses(new Class<?>[]{ STITree.class});
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        return SparkSession.builder().config(jsc.getConf()).getOrCreate();
    }
}

