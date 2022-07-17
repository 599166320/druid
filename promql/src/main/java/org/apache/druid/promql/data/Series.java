package org.apache.druid.promql.data;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class Series implements Serializable {
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


    public String createKey(){
        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String,Object> lb : labels.entrySet()){
            builder.append(lb.getKey()).append(",").append(lb.getValue()).append(",");
        }
        return builder.toString();
    }
}
