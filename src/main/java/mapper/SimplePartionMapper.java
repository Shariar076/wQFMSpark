package mapper;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.Collections;
import java.util.Iterator;

public class SimplePartionMapper implements MapPartitionsFunction<Row, Integer> {
    @Override
    public Iterator<Integer> call(Iterator<Row> iterator) throws Exception {
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        return Collections.singletonList(count).iterator();
    }
}
