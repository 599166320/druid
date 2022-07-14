package org.apache.druid.promql.logical;
import org.apache.druid.promql.data.Series;
import java.util.*;

public class SumAggregationOperator extends AggregationOperator{
    @Override
    public Operator call() {
        //不同序列，更加相同label做分组聚合
        Map<String,TreeMap<Integer,Double>> dataPointMap = new HashMap<>();
        for(Series series:seriesSet){
            List<String> groubValue = new ArrayList<>();
            for(String groubBy:groubBys){
                groubValue.add(series.getLabels().get(groubBy).toString());
            }
            String key = "";
            if(groubValue.size() > 0){
                key = String.join(",",groubValue);
            }
            dataPointMap.merge(key,series.getDataPoint(),(o,n)->{
                for (Map.Entry<Integer,Double> e:n.entrySet()){
                    o.merge(e.getKey(),e.getValue(),(o_p,n_p)->{
                        return o_p+n_p;
                    });
                }
                return o;
            });
        }
        SeriesSetOperator seriesSetOperator = new SeriesSetOperator();
        seriesSet = new ArrayList<>();
        for(Map.Entry<String,TreeMap<Integer,Double>> e:dataPointMap.entrySet()){
            String[] vs = e.getKey().split(",");
            TreeMap<String,Object> labels = new TreeMap<>();
            if(groubBys.size() > 0){
                for(int i = 0;i < vs.length; i++){
                    labels.put(groubBys.get(i),vs[i]);
                }
            }
            Series series = new Series();
            series.setLabels(labels);
            series.setDataPoint(e.getValue());
            seriesSet.add(series);
        }
        seriesSetOperator.setSeriesSet(seriesSet);
        return seriesSetOperator;
    }
}
