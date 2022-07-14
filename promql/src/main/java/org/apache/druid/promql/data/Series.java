package org.apache.druid.promql.data;
import java.util.TreeMap;

public class Series {
    private TreeMap<Integer,Double> dataPoint;
    private TreeMap<String,Object> labels = new TreeMap<>();

    public TreeMap<Integer, Double> getDataPoint() {
        return dataPoint;
    }

    public void setDataPoint(TreeMap<Integer, Double> dataPoint) {
        this.dataPoint = dataPoint;
    }

    public TreeMap<String, Object> getLabels() {
        return labels;
    }

    public void setLabels(TreeMap<String, Object> labels) {
        this.labels = labels;
    }


}
