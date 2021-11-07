package mapper;

import algorithm.wQFMRunner;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import structure.TreeTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class QuartetToTreeTablePartitionMapper implements MapPartitionsFunction<Row, TreeTable> {
    @Override
    public Iterator<TreeTable> call(Iterator<Row> iterator) throws Exception {
        TreeTable treeTable = new TreeTable();
        int qtCount = 0;
        double qtWeightSum = 0;
        StringBuilder qtTag = new StringBuilder();
        ArrayList<String> arrayList = new ArrayList<>();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            arrayList.add(row.getString(0));
            // qtCount++;
            qtWeightSum += Double.parseDouble(row.getString(2)); //row.getDouble(2);
            if (qtTag.length() == 0) qtTag = new StringBuilder(row.getString(1)); // find this partition's tag
            else if (!qtTag.toString().contains(row.getString(1))) qtTag.append('|').append(row.getString(1));
            // throw new Exception("Tag mismatch within partition, expected: "+ qtTag+" , got: "+row.getString(1));
        }
        String tree = "<NULL>";
        if (arrayList.size() > 0) tree = new wQFMRunner().runDevideNConquer(arrayList, qtTag.toString()); //String.valueOf(arrayList.size());

        treeTable.setTree(tree);
        treeTable.setTag(qtTag.toString());
        treeTable.setSupport(qtWeightSum);
        return Collections.singletonList(treeTable).iterator();

    }
}
