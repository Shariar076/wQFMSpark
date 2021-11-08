package reducer;

import org.apache.spark.api.java.function.ReduceFunction;
import properties.ConfigValues;
import structure.TaxaTable;

import java.util.ArrayList;
import java.util.Collections;

public class TaxaTableReducer implements ReduceFunction<TaxaTable> {
    @Override
    public TaxaTable call(TaxaTable taxaTable, TaxaTable t1) throws Exception {
        for (String taxon : t1.TAXA_LIST) {
            taxaTable.updateTaxaTableWithTaxon(taxon);
        }
        // v1
        // updateTaxaTablePartitionsByTaxaListV1(t1.TAXA_LIST, taxaTable);
        // v2
        if (t1.TAXA_PARTITION_LIST.isEmpty()) taxaTable.updateTaxaTablePartitionsByTaxaListV2(t1.TAXA_LIST);  // t1: no reduce yet
        else taxaTable.updateTaxaTablePartitionsByTaxaTablePartitionsV2(t1); // t1: already reduced
        // v3
        // if (t1.TAXA_LIST.size() > 4) updateTaxaTablePartitionsByTaxaListV3(taxaTable.TAXA_LIST, taxaTable);
        return taxaTable;
    }
}
