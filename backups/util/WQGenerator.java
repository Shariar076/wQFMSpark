package util;

import properties.Config;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WQGenerator {
    private static final String tempFileName = "temp-gt.tre";
    private static String extractQuartet(String rawQuartet) throws Exception {
        Pattern quartetPattern = Pattern.compile(", [a-z]+: ([0-9a-zA-Z]+) ([0-9a-zA-Z]+) \\| ([0-9a-zA-Z]+) ([0-9a-zA-Z]+)");

        Matcher matcher = quartetPattern.matcher(rawQuartet);

        if (matcher.find()) {
            return String.format("((%s,%s),(%s,%s));",
                    matcher.group(1),
                    matcher.group(2),
                    matcher.group(3),
                    matcher.group(4));
        } else {
            throw new Exception("No regex match found in " + rawQuartet);
        }
    }

    private static ArrayList<String> runTS() throws Exception {
        String cmd = "triplets.soda2103 printQuartets" + " " + tempFileName;
        ArrayList<String> lines = new ArrayList<>();
        ArrayList<String> errors = new ArrayList<>();
        Process p = Runtime.getRuntime().exec(cmd);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        String s;

        while ((s = stdInput.readLine()) != null) {
            lines.add(extractQuartet(s));
        }

        while ((s = stdError.readLine()) != null) {
            errors.add(s);
        }

        if (!errors.isEmpty()){
            throw new Exception(errors.toString());
        }

        return lines;
    }

    private static ArrayList<String> induceQuartetsFromTree(String tree) throws Exception {
        // String tempFileName = WQGenerator.class.getClassLoader().getResource("temp-gt.tre").getPath();
        FileWriter fileWriter = new FileWriter(tempFileName);
        fileWriter.write(tree);
        fileWriter.close();

        return runTS();
    }

    private static void removeTempFile(){
        File myObj = new File(tempFileName);
        if (!myObj.delete()) {
            System.out.println("Failed to delete the temp file.");
        }
    }

    private static ArrayList<String> udf(String line) throws Exception {
        throw new Exception("My Exception");
        // ArrayList<String> ret = new ArrayList<>();
        // for(String w: line.split(",")){
        //     ret.add(w);
        // }
        // return ret;
    }


    public static void generateWQ(String inputFilename, String outputFilename) {
        // RDD
        // JavaRDD<String> textFile = spark.textFile("hdfs://localhost:9000/user/himel/"+inputFilename);
        // JavaPairRDD<String, Integer> counts = textFile
        //         .flatMap(s -> induceQuartetsFromTree(s).iterator())
        //         .mapToPair(word -> new Tuple2<>(word, 1))
        //         .reduceByKey((a, b) -> a + b)
        //         .mapToPair(p -> p.swap())
        //         .sortByKey(false, 1)
        //         .mapToPair(p -> p.swap())
        //         ;
        // System.out.println(counts.collect());
        // counts.saveAsTextFile("hdfs://localhost:9000/user/himel/"+outputFilename);
        // Dataframe
        Dataset<Row> stDf = Config.SPARK.read().text(Config.HDFS_PATH + inputFilename);
        Dataset<String> qtDs = stDf.flatMap((FlatMapFunction<Row, String>)
                        r -> induceQuartetsFromTree(r.toString().replaceAll("^\\[|\\]$","")).iterator(),
                Encoders.STRING());

        removeTempFile();
        Dataset<Row> weightedQuartets = qtDs.toDF().groupBy("value").count();
        weightedQuartets.write().mode("overwrite").option("header","true")
                .csv(Config.HDFS_PATH+"/"+outputFilename);

    }
}
