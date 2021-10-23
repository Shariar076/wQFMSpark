package structure;

import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class InitialTable {
    public static Map<String, Integer> map_of_str_vs_int_tax_list = new HashMap<>(); //for forward checking
    public static Map<Integer, String> map_of_int_vs_str_tax_list = new HashMap<>(); //for back conversion
    public static int TAXA_COUNT = 0;
    public static ArrayList<Integer> TAXA_LIST = new ArrayList<Integer>();
    public static Dataset<Quartet> quartetsTable;

    public InitialTable(boolean flag) {
    }
    // public int getTableSize() { return  (int) quartetsTable.count();}
    //
    // public void initializeQuartetsTable(Dataset<LegacyQuartet> quartetsTable){
    //     LegacyInitialTable.quartetsTable = quartetsTable;
    // }
    // public void showQuartetsTable(){
    //     // System.out.println(this.quartetsTable.select("weight").collectAsList());
    //     quartetsTable.show();
    //     // this.quartetsTable.toDF().map((MapFunction<Row, String>)
    //     //         r -> ">>"+r.toString(), Encoders.STRING()).show(false);
    //     // System.out.println("map_of_int_vs_str_tax_list: " + map_of_int_vs_str_tax_list);
    //     // System.out.println("map_of_int_vs_str_tax_list: " + map_of_int_vs_str_tax_list);
    // }

    @Override
    public String toString() {
        return "LegacyInitialTable{" + "list_quartets=" + quartetsTable + '}';
    }

    // public LegacyQuartet get(int idx) {
    //     Dataset<Row> indexedQuartetsTable = quartetsTable.toDF().withColumn("id",monotonically_increasing_id());
    //     indexedQuartetsTable.show();
    //
    //
    //     //This mapper wont work
    //     Dataset<LegacyQuartet> quartets = indexedQuartetsTable.select(indexedQuartetsTable.col("id").equalTo(idx)).map(new QuartetMapper(), Encoders.bean(LegacyQuartet.class));
    //     quartets.show();
    //     return quartets.first();
    // }
    // public void assignByReference(LegacyInitialTable initialTable) {
    //     this.list_quartets = initialTable.list_quartets; //assign by reference
    // }
}
