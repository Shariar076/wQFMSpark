package mappers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import structures.InitialTable;
import structures.Quartet;

public class QuartetStringMapper implements MapFunction<Row, Quartet> {
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
            a = InitialTable.TAXA_COUNTER;
            InitialTable.TAXA_COUNTER++;
            InitialTable.map_of_str_vs_int_tax_list.put(arr[0], a);
            InitialTable.map_of_int_vs_str_tax_list.put(a, arr[0]);
        }

        if (InitialTable.map_of_str_vs_int_tax_list.containsKey(arr[1])) {
            b = InitialTable.map_of_str_vs_int_tax_list.get(arr[1]);
        } else { //THIS taxon doesn't exist.
            b = InitialTable.TAXA_COUNTER;
            InitialTable.TAXA_COUNTER++;
            InitialTable.map_of_str_vs_int_tax_list.put(arr[1], b);
            InitialTable.map_of_int_vs_str_tax_list.put(b, arr[1]);
        }
        if (InitialTable.map_of_str_vs_int_tax_list.containsKey(arr[2])) {
            c = InitialTable.map_of_str_vs_int_tax_list.get(arr[2]);
        } else { //THIS taxon doesn't exist.
            c = InitialTable.TAXA_COUNTER;
            InitialTable.TAXA_COUNTER++;
            InitialTable.map_of_str_vs_int_tax_list.put(arr[2], c);
            InitialTable.map_of_int_vs_str_tax_list.put(c, arr[2]);
        }
        if (InitialTable.map_of_str_vs_int_tax_list.containsKey(arr[3])) {
            d = InitialTable.map_of_str_vs_int_tax_list.get(arr[3]);
        } else { //THIS taxon doesn't exist.
            d = InitialTable.TAXA_COUNTER;
            InitialTable.TAXA_COUNTER++;
            InitialTable.map_of_str_vs_int_tax_list.put(arr[3], d);
            InitialTable.map_of_int_vs_str_tax_list.put(d, arr[3]);
        }

        quartet.initialiseQuartet(a, b, c, d, Double.parseDouble(arr[4]));
        return quartet;
    }
}
