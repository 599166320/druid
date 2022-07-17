package org.apache.druid.promql.logical;
import io.vavr.Tuple2;
import org.apache.druid.promql.data.Series;
import java.util.*;
public class SeriesSetOperator implements Operator{

    private List<Series> seriesSet = new ArrayList<>();

    public List<Series> getSeriesSet() {
        return seriesSet;
    }

    public void setSeriesSet(List<Series> seriesSet) {
        this.seriesSet = seriesSet;
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    //matrix,verctor
    private String resultType;

    @Override
    public Operator call() throws Exception {
        return this;
    }



    /****************************** add start **************************************/
    public Operator add(NumberLiteralOperator numberLiteralOperator){
        for(Series series:seriesSet){
            TreeMap<Integer, Double> datapoints = series.getDataPoint();
            for(Map.Entry<Integer,Double> entry : datapoints.entrySet()){
                entry.setValue(numberLiteralOperator.getNumber().doubleValue()+entry.getValue());
            }
        }
        return this;
    }

    public Operator add(SeriesSetOperator other){
        List<Series> otherSeriesSet = other.getSeriesSet();
        List<Series> thisSeriesSet = this.getSeriesSet();
        Map<String,Series> newSeriesSetMap = new HashMap<>();
        //label完全相同才需要合并
        for(Series ot:otherSeriesSet){
            String key = ot.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(add(ot,ss));
            }else{
                newSeriesSetMap.put(key,ot);
            }
        }
        for(Series t:thisSeriesSet){
            String key = t.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(add(t,ss));
            }else{
                newSeriesSetMap.put(key,t);
            }
        }
        this.setSeriesSet(new ArrayList<>(newSeriesSetMap.values()));
        return this;
    }

    private TreeMap<Integer, Double> add(Series ss1,Series ss2){
        TreeMap<Integer, Double> dps1 = ss1.getDataPoint();
        TreeMap<Integer, Double> dps2 = ss2.getDataPoint();
        for(Map.Entry<Integer, Double> entry:dps2.entrySet()){
            dps1.merge(entry.getKey(),entry.getValue(),(v1,v2)->{
                return v1+v2;
            });
        }
        return dps1;
    }
    /****************************** add end **************************************/



    /****************************** sub start **************************************/
    public static Operator sub(SeriesSetOperator ths,NumberLiteralOperator numberLiteralOperator){
        for(Series series:ths.seriesSet){
            TreeMap<Integer, Double> datapoints = series.getDataPoint();
            for(Map.Entry<Integer,Double> entry : datapoints.entrySet()){
                entry.setValue(entry.getValue()-numberLiteralOperator.getNumber().doubleValue());
            }
        }
        return ths;
    }

    public static Operator sub(NumberLiteralOperator numberLiteralOperator,SeriesSetOperator ths){
        for(Series series:ths.seriesSet){
            TreeMap<Integer, Double> datapoints = series.getDataPoint();
            for(Map.Entry<Integer,Double> entry : datapoints.entrySet()){
                entry.setValue(numberLiteralOperator.getNumber().doubleValue()-entry.getValue());
            }
        }
        return ths;
    }


    public Operator sub(SeriesSetOperator other){
        List<Series> otherSeriesSet = other.getSeriesSet();
        List<Series> thisSeriesSet = this.getSeriesSet();
        Map<String,Series> newSeriesSetMap = new HashMap<>();
        //label完全相同才需要合并
        for(Series ot:otherSeriesSet){
            String key = ot.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(sub(ot,ss));
            }else{
                newSeriesSetMap.put(key,ot);
            }
        }
        for(Series t:thisSeriesSet){
            String key = t.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(sub(t,ss));
            }else{
                newSeriesSetMap.put(key,t);
            }
        }
        this.setSeriesSet(new ArrayList<>(newSeriesSetMap.values()));
        return this;
    }

    private TreeMap<Integer, Double> sub(Series ss1,Series ss2){
        TreeMap<Integer, Double> dps1 = ss1.getDataPoint();
        TreeMap<Integer, Double> dps2 = ss2.getDataPoint();
        for(Map.Entry<Integer, Double> entry:dps2.entrySet()){
            dps1.merge(entry.getKey(),entry.getValue(),(v1,v2)->{
                return v1-v2;
            });
        }
        return dps1;
    }
    /****************************** sub end **************************************/



    /****************************** mul start **************************************/
    public Operator mul(NumberLiteralOperator numberLiteralOperator){
        for(Series series:seriesSet){
            TreeMap<Integer, Double> datapoints = series.getDataPoint();
            for(Map.Entry<Integer,Double> entry : datapoints.entrySet()){
                entry.setValue(numberLiteralOperator.getNumber().doubleValue()*entry.getValue());
            }
        }
        return this;
    }

    public Operator mul(SeriesSetOperator other){
        List<Series> otherSeriesSet = other.getSeriesSet();
        List<Series> thisSeriesSet = this.getSeriesSet();
        Map<String,Series> newSeriesSetMap = new HashMap<>();
        //label完全相同才需要合并
        for(Series ot:otherSeriesSet){
            String key = ot.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(mul(ot,ss));
            }else{
                newSeriesSetMap.put(key,ot);
            }
        }
        for(Series t:thisSeriesSet){
            String key = t.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(mul(t,ss));
            }else{
                newSeriesSetMap.put(key,t);
            }
        }
        this.setSeriesSet(new ArrayList<>(newSeriesSetMap.values()));
        return this;
    }

    private TreeMap<Integer, Double> mul(Series ss1,Series ss2){
        TreeMap<Integer, Double> dps1 = ss1.getDataPoint();
        TreeMap<Integer, Double> dps2 = ss2.getDataPoint();
        for(Map.Entry<Integer, Double> entry:dps2.entrySet()){
            dps1.merge(entry.getKey(),entry.getValue(),(v1,v2)->{
                return v1*v2;
            });
        }
        return dps1;
    }
    /****************************** mul end **************************************/







    /****************************** div start **************************************/
    public static Operator div(SeriesSetOperator ths,NumberLiteralOperator numberLiteralOperator){
        for(Series series:ths.seriesSet){
            TreeMap<Integer, Double> datapoints = series.getDataPoint();
            for(Map.Entry<Integer,Double> entry : datapoints.entrySet()){
                entry.setValue(entry.getValue()/numberLiteralOperator.getNumber().doubleValue());
            }
        }
        return ths;
    }

    public static Operator div(NumberLiteralOperator numberLiteralOperator,SeriesSetOperator ths){
        for(Series series:ths.seriesSet){
            TreeMap<Integer, Double> datapoints = series.getDataPoint();
            for(Map.Entry<Integer,Double> entry : datapoints.entrySet()){
                entry.setValue(numberLiteralOperator.getNumber().doubleValue()/entry.getValue());
            }
        }
        return ths;
    }


    public Operator div(SeriesSetOperator other){
        List<Series> otherSeriesSet = other.getSeriesSet();
        List<Series> thisSeriesSet = this.getSeriesSet();
        Map<String,Series> newSeriesSetMap = new HashMap<>();
        //label完全相同才需要合并
        for(Series ot:otherSeriesSet){
            String key = ot.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(div(ot,ss));
            }else{
                newSeriesSetMap.put(key,ot);
            }
        }
        for(Series t:thisSeriesSet){
            String key = t.createKey();
            if(newSeriesSetMap.containsKey(key)){
                Series ss = newSeriesSetMap.get(key);
                ss.setDataPoint(div(t,ss));
            }else{
                newSeriesSetMap.put(key,t);
            }
        }
        this.setSeriesSet(new ArrayList<>(newSeriesSetMap.values()));
        return this;
    }

    private TreeMap<Integer, Double> div(Series ss1,Series ss2){
        TreeMap<Integer, Double> dps1 = ss1.getDataPoint();
        TreeMap<Integer, Double> dps2 = ss2.getDataPoint();
        for(Map.Entry<Integer, Double> entry:dps2.entrySet()){
            dps1.merge(entry.getKey(),entry.getValue(),(v1,v2)->{
                return v1/v2;
            });
        }
        return dps1;
    }
    /****************************** div end **************************************/


    public SeriesSetOperator vectorSelectorSingle(int step){
        if(step > 0){
            for(Series series : seriesSet){
                TreeMap<Integer, Double> datapoints = series.getDataPoint();
                TreeMap<Integer, Double> newDatapoints = new TreeMap<>();
                if(datapoints.size() > 0){
                    Map.Entry<Integer, Double>[] entries = new Map.Entry[datapoints.entrySet().size()];
                    datapoints.entrySet().toArray(entries);
                    Integer ts = entries[0].getKey();
                    Integer endTime = entries[entries.length - 1].getKey();
                    int idx = 0;
                    while (ts <= endTime){
                        Tuple2<Integer,Double> idxAndv = findValue(idx,ts,step,entries);
                        if(Objects.nonNull(idxAndv)){
                            idx = idxAndv._1;
                            newDatapoints.put(ts,idxAndv._2);
                            ts +=  step;
                        }else {
                            break;
                        }
                    }
                }
                series.setDataPoint(newDatapoints);
            }
        }
        return this;
    }

    private Tuple2<Integer,Double> findValue(int idx,Integer ts, int step, Map.Entry<Integer, Double>[] entries){
        for(int i = idx;i < entries.length;i++){
            if(Objects.equals(entries[i].getKey(), ts)){
                return new Tuple2<>(i,entries[i].getValue());
            }else if(entries[i].getKey() > ts){
                return new Tuple2<>(i-1,entries[i-1].getValue());
            }
        }
        return null;
    }

}
