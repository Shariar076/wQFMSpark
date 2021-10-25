package algorithm;

import config.Config;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.In;
import structure.InitialTable;

import java.util.*;

public class Sample {
    public static void testMapReduce() {
        System.out.println("Testing Map Reduce");
        // System.out.println("Num Partitions: " + InitialTable.quartetsTable.toJavaRDD().getNumPartitions());
        SampleMapper sampleMapper = new SampleMapper();
        SampleReducer sampleReducer = new SampleReducer();
        Dataset<SampleMapper> df = InitialTable.quartetsTable
                .toDF()
                // .coalesce(1)
                .map
                        (
                                (MapFunction<Row, SampleMapper>) sampleMapper::incrementCount,
                                Encoders.bean(SampleMapper.class)
                        )
                // .toDF()
                ;


        // ArrayList<SampleReducer> sampleReducers = new ArrayList<>();
        // for (int i = 1; i < 556; i++) {
        //     SampleReducer mr = new SampleReducer();
        //     mr.count=i;
        //     sampleReducers.add(mr);
        // }
        // Dataset<SampleReducer> df = Config.SPARK.createDataset(sampleReducers, Encoders.bean(SampleReducer.class)).repartition(6);
        df.show((int) df.count());
        // System.out.println(df.javaRDD().getNumPartitions());

        Encoder<SampleReducer> reducerEncoder = Encoders.bean(SampleReducer.class);
        Dataset<SampleReducer> df2 = df.as(reducerEncoder);
        SampleReducer sampleReducer1 = df2.reduce((ReduceFunction<SampleReducer>) sampleReducer::addCount);
        System.out.println(sampleReducer1.count);

        df.show((int) df.count());
        Dataset<Integer> df1 = df.as(Encoders.INT());
        Integer mapReduceInt = df1.reduce((ReduceFunction<Integer>) sampleReducer::addCount);
        System.out.println(mapReduceInt);

        int count = 0;
        for(SampleMapper sampleMapper1 : df.collectAsList()){
            count+= sampleMapper1.count;
        }
        System.out.println(count);
    }

    public static void mapPartitionExample(){
        SampleMapper sampleMapper = new SampleMapper();
        SampleReducer sampleReducer = new SampleReducer();
        Dataset<SampleMapper> df = InitialTable.quartetsTable
                .toDF()
                // .coalesce(1)
                .map
                        (
                                (MapFunction<Row, SampleMapper>) sampleMapper::incrementCount,
                                Encoders.bean(SampleMapper.class)
                        )
                // .toDF()
                ;
        Dataset<Integer> df1 = df.as(Encoders.INT());
        JavaRDD<Integer> objectJavaRDD = df1.toJavaRDD().mapPartitions(iterator -> {
            int size =0;
            while(iterator.hasNext()) {
                size++;
                iterator.next();
            }
            return Collections.singletonList(size).iterator();
        });
        System.out.println(objectJavaRDD.collect());
        Dataset<Integer> objectDataset = Config.SPARK.createDataset(objectJavaRDD.rdd(), Encoders.INT());
        objectDataset.show();

        Dataset<Row> objectDataframe = objectDataset.toDF(); //Config.SPARK.createDataFrame(objectJavaRDD.rdd(), Integer.class); this only work with user defined class apparently
        objectDataframe.show();
    }
    public static String recursiveDivideAndConquer(int level) {
        // Broadcast<String> broadCast = Config.SPARK.sparkContext()
        //         .broadcast("Broadcasted", scala.reflect.ClassManifestFactory.fromClass(String.class));
        // System.out.println(broadCast.getValue());
        String finalTree = "NONE";
        LegacyBipartition legBip = new LegacyBipartition();
        // legBip.runDevideNConquer(InitialTable.quartetsTable.collectAsList());

        // Initial Bipartition
        // InitialBipartition initialBipartition = new InitialBipartition(InitialTable.TAXA_LIST);
        // //monotonically_increasing_id Doesn't ensure sequenciallity but ensures increasing values; good enough
        // // should be #breaks=#partitions
        // Dataset<Row> allPartitionDs = InitialTable.quartetsTable
        //         .toDF()
        //         .map((MapFunction<Row, InitialBipartition>) initialBipartition::performPartitionBasedOnQuartet, Encoders.bean(InitialBipartition.class))
        //         .toDF()
        //         .withColumn("id", monotonically_increasing_id())
        //         .orderBy(col("id").desc());
        // allPartitionDs.select(col("partitionMap"), col("id")).show((int)allPartitionDs.count(), false);
        // Map<Integer, Integer> latestPartition = allPartitionDs.head().getJavaMap(2);
        // System.out.println("initial bipartition after performPartitionBasedOnQuartet: " + latestPartition);
        // Map<Integer, Integer> initialBipartitionMap = initialBipartition.performPartitionRandomBalanced(latestPartition);
        // System.out.println("initial bipartition after performPartitionRandomBalanced: " + initialBipartitionMap);
        //
        // // legBip.doBipartition8ValuesCalculation(InitialTable.quartetsTable,initialBipartitionMap, level);
        // // Bipartition scores
        // Bipartition8Values initialBip_8_vals = new Bipartition8Values(initialBipartitionMap);
        //
        // Encoder<Bipartition8Values> bipartition8ValuesEncoder = Encoders.bean(Bipartition8Values.class);
        //
        // initialBip_8_vals = InitialTable.quartetsTable
        //         .toDF()
        //         .map((MapFunction<Row, Bipartition8Values>) initialBip_8_vals::compute8ValuesUsingAllQuartets_this_level, Encoders.bean(Bipartition8Values.class))
        //         .toDF()
        //         .withColumn("id", monotonically_increasing_id())
        //         .orderBy(col("id").desc()).as(bipartition8ValuesEncoder)
        //         .head();
        // // System.out.println(initialBip_8_vals);
        // System.out.println(initialBip_8_vals.map_four_tax_seq_weights_list);
        // if (Config.PARTITION_SCORE_MODE == DefaultValues.PARTITION_SCORE_FULL_DYNAMIC) {
        //     initialBip_8_vals.calculateDynamicScore(level, initialBip_8_vals.getMap_four_tax_seq_weights_list());
        // }

        return finalTree;
    }
}
