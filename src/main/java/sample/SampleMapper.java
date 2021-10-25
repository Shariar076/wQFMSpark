package sample;

import org.apache.spark.sql.Row;

import java.io.Serializable;

public class SampleMapper implements Serializable {
    public int count;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public SampleMapper incrementCount(Row row){
        this.count++;
        return this;
    }
}
