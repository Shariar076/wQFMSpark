package algorithm;

import java.io.Serializable;

public class SampleReducer implements Serializable {

    public int count;

    public SampleReducer() {
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Integer addCount(Integer integer1, Integer integer2) {
        return integer1+integer2;
    }

    public SampleReducer addCount(SampleReducer sampleReducer1, SampleReducer sampleReducer2) {
        // Problem sum gives actual_value+5 when using class
        // reduce works like curr_reduce(prev_reduce(v1, v2), v3)
        SampleReducer ret = new SampleReducer();
        ret.count = sampleReducer1.count+ sampleReducer2.count;

        return ret;
    }
    // public Row addCount(Row row1, Row row2){
    //     this.count = row1.getInt(0)+row2.getInt(0);
    //     ArrayList<Integer> retList = new ArrayList<>();
    //     retList.add(row1.getInt(0));
    //     retList.add(row2.getInt(0));
    //     retList.add(this.count);
    //     return RowFactory.create(retList.toArray());
    // }
}
