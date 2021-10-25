package mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import structure.InitialTableSpark;
import structure.SerialQuartet;

public class StringToQuartetMapper implements MapFunction<Row, SerialQuartet> {
    private static void updateInitialTableWithTaxon(String taxonOrg, int taxonMapped){
        InitialTableSpark.TAXA_COUNT++;
        InitialTableSpark.TAXA_LIST.add(taxonMapped);
        InitialTableSpark.map_of_str_vs_int_tax_list.put(taxonOrg, taxonMapped);
        InitialTableSpark.map_of_int_vs_str_tax_list.put(taxonMapped, taxonOrg);
    }
    @Override
    public SerialQuartet call(Row row) throws Exception {
        // String quartetString = row.getAs("value");
        // String count = row.getAs("count"); //String.valueOf(row.getAs("value"));
        String s = row.getAs("value")+" "+row.getAs("count");
        SerialQuartet quartet= new SerialQuartet();
        s = s.replace(" ", "");
        s = s.replace(";", ",");
        s = s.replace("(", "");
        s = s.replace(")", ""); // Finally end up with A,B,C,D,41.0
        String[] arr = s.split(",");

        int a, b, c, d;
        if (InitialTableSpark.map_of_str_vs_int_tax_list.containsKey(arr[0])) {
            a = InitialTableSpark.map_of_str_vs_int_tax_list.get(arr[0]);
        } else { //THIS taxon doesn't exist.
            a = InitialTableSpark.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[0], a);
        }

        if (InitialTableSpark.map_of_str_vs_int_tax_list.containsKey(arr[1])) {
            b = InitialTableSpark.map_of_str_vs_int_tax_list.get(arr[1]);
        } else { //THIS taxon doesn't exist.
            b = InitialTableSpark.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[1], b);
        }
        if (InitialTableSpark.map_of_str_vs_int_tax_list.containsKey(arr[2])) {
            c = InitialTableSpark.map_of_str_vs_int_tax_list.get(arr[2]);
        } else { //THIS taxon doesn't exist.
            c = InitialTableSpark.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[2], c);
        }
        if (InitialTableSpark.map_of_str_vs_int_tax_list.containsKey(arr[3])) {
            d = InitialTableSpark.map_of_str_vs_int_tax_list.get(arr[3]);
        } else { //THIS taxon doesn't exist.
            d = InitialTableSpark.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[3], d);
        }

        quartet.initialiseQuartet(a, b, c, d, Double.parseDouble(arr[4]));
        return quartet;
    }
}
