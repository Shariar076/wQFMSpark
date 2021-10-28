package mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import structure.TaxaTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class StringToTaxaTableMapper implements MapFunction<Row, TaxaTable> {
    private static void updateTaxaTableWithTaxon(String taxon, TaxaTable taxaTable) {
        if (!taxaTable.TAXA_LIST.contains(taxon)) {
            taxaTable.TAXA_COUNT++;
            taxaTable.TAXA_LIST.add(taxon);
        }

    }

    // private static void updateTaxaTableWithTaxaList(String[] taxaList, TaxaTable taxaTable) {
    //     Arrays.sort(taxaList);
    //     String taxaListStr = String.join("", taxaList);
    //     if (!taxaTable.TAXA_GRP_COUNT.keySet().contains(taxaListStr)) {
    //         taxaTable.TAXA_GRP_COUNT.put(taxaListStr, 0);
    //     }
    //     taxaTable.TAXA_GRP_COUNT.put(taxaListStr, taxaTable.TAXA_GRP_COUNT.get(taxaListStr) + 1);
    //
    // }

    @Override
    public TaxaTable call(Row row) throws Exception {
        TaxaTable taxaTable = new TaxaTable();
        String s = row.getAs("value") + " " + row.getAs("count");
        s = s.replace(" ", "");
        s = s.replace(";", ",");
        s = s.replace("(", "");
        s = s.replace(")", ""); // Finally end up with A,B,C,D,41.0
        String[] arr = s.split(",");

        for (int i = 0; i < 4; i++) {
            updateTaxaTableWithTaxon(arr[i], taxaTable);
        }
        // updateTaxaTableWithTaxaList(arr, taxaTable);
        return taxaTable;
    }
}
