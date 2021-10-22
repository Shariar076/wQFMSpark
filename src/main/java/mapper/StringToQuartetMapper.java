package mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import structure.InitialTable;
import structure.Quartet;

public class StringToQuartetMapper implements MapFunction<Row, Quartet> {
    private static void updateInitialTableWithTaxon(String taxonOrg, int taxonMapped){
        InitialTable.TAXA_COUNT++;
        InitialTable.TAXA_LIST.add(taxonMapped);
        InitialTable.map_of_str_vs_int_tax_list.put(taxonOrg, taxonMapped);
        InitialTable.map_of_int_vs_str_tax_list.put(taxonMapped, taxonOrg);
    }
    @Override
    public Quartet call(Row row) throws Exception {
        // String quartetString = row.getAs("value");
        // String count = row.getAs("count"); //String.valueOf(row.getAs("value"));
        String s = row.getAs("value")+" "+row.getAs("count");
        Quartet quartet= new Quartet();
        s = s.replace(" ", "");
        s = s.replace(";", ",");
        s = s.replace("(", "");
        s = s.replace(")", ""); // Finally end up with A,B,C,D,41.0
        String[] arr = s.split(",");

        int a, b, c, d;
        if (InitialTable.map_of_str_vs_int_tax_list.containsKey(arr[0])) {
            a = InitialTable.map_of_str_vs_int_tax_list.get(arr[0]);
        } else { //THIS taxon doesn't exist.
            a = InitialTable.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[0], a);
        }

        if (InitialTable.map_of_str_vs_int_tax_list.containsKey(arr[1])) {
            b = InitialTable.map_of_str_vs_int_tax_list.get(arr[1]);
        } else { //THIS taxon doesn't exist.
            b = InitialTable.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[1], b);
        }
        if (InitialTable.map_of_str_vs_int_tax_list.containsKey(arr[2])) {
            c = InitialTable.map_of_str_vs_int_tax_list.get(arr[2]);
        } else { //THIS taxon doesn't exist.
            c = InitialTable.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[2], c);
        }
        if (InitialTable.map_of_str_vs_int_tax_list.containsKey(arr[3])) {
            d = InitialTable.map_of_str_vs_int_tax_list.get(arr[3]);
        } else { //THIS taxon doesn't exist.
            d = InitialTable.TAXA_COUNT;
            updateInitialTableWithTaxon(arr[3], d);
        }

        quartet.initialiseQuartet(a, b, c, d, Double.parseDouble(arr[4]));
        return quartet;
    }
}
