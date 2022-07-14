package org.apache.druid.promql.logical;
import java.util.*;
import java.util.concurrent.FutureTask;
public class InstantOperator implements Operator{
    //protected String key;
    protected String metric;
    protected List<Map<String,Object>> where;
    protected Map<String, Object> props;
    protected FutureTask<Operator> futureTask;
    private SeriesSetOperator seriesSetOperator;
    public String getMetric() {
        return metric;
    }


    public void setMetric(String metric) {
        this.metric = metric;
    }

    public List<Map<String, Object>> getWhere() {
        return where;
    }

    public void setWhere(List<Map<String, Object>> where) {
        this.where = where;
    }

    public Map<String, Object> getProps() {
        return props;
    }

    public void setProps(Map<String, Object> props) {
        this.props = props;
    }

    public SeriesSetOperator getSeriesSetOperator() {
        return seriesSetOperator;
    }

    public void setSeriesSetOperator(SeriesSetOperator seriesSetOperator) {
        this.seriesSetOperator = seriesSetOperator;
    }

    @Override
    public Operator call() throws Exception {
        //Operator operator = futureTask.get();
        return seriesSetOperator;
    }

    /*
    public void startExec(ExecutorService executorService){
        futureTask = new FutureTask<Operator>(()->{
            //执行sql查询
            SeriesSetOperator seriesSetOperator = new SeriesSetOperator();
            List<Series> seriesSet = new ArrayList<>();
            seriesSetOperator.setSeriesSet(seriesSet);
            Map<String,Series> seriesMap = new HashMap<>();

            int i = 0;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/gz00018ml/myproject/druid/promql/src/main/resources/quantile.json")))){
                String line;
                while ((line = br.readLine()) != null){
                    JSONObject obj = JSON.parseObject(line);
                    String instance = obj.getString("instance");
                    String topic = obj.getString("topic");
                    String consumergroup = obj.getString("consumergroup");
                    String le = obj.getString("le");
                    int time = obj.getIntValue("time");
                    double value = obj.getDoubleValue("value");
                    value = (i++/32)*value+value;

                    String  k = instance+","+topic+","+consumergroup+","+le;
                    if(seriesMap.containsKey(k)){
                        Series series = seriesMap.get(k);
                        series.getDataPoint().put(time,value);
                    }else {
                        TreeMap<String,Object> lb = new TreeMap<>();
                        lb.put("instance",instance);
                        lb.put("topic",topic);
                        lb.put("consumergroup",consumergroup);
                        lb.put("le",le);

                        TreeMap<Integer,Double> dp = new TreeMap<>();
                        dp.put(time,value);
                        Series series = new Series();
                        series.setLabels(lb);
                        series.setDataPoint(dp);
                        seriesSet.add(series);
                        seriesMap.put(k,series);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            return seriesSetOperator;
        });
        executorService.execute(futureTask);
    }

    public void startExec11111(ExecutorService executorService){
        futureTask = new FutureTask<Operator>(()->{
            //执行sql查询
            SeriesSetOperator seriesSetOperator = new SeriesSetOperator();
            List<Series> seriesSet = new ArrayList<>();
            seriesSetOperator.setSeriesSet(seriesSet);
            Series series = new Series();
            TreeMap<String,Object> labels = new TreeMap<>();
            labels.put("instance","$instance");
            labels.put("topic","$topic");
            labels.put("consumergroup","$consumergroup01");

            TreeMap<Integer,Double> dataPoint = new TreeMap<>();
            dataPoint.put(1657504800,1.0);
            dataPoint.put(1657505100,2.0);
            dataPoint.put(1657505400,3.0);
            dataPoint.put(1657505700,4.0);
            dataPoint.put(1657506000,5.0);
            dataPoint.put(1657506300,6.0);
            dataPoint.put(1657506600,7.0);
            dataPoint.put(1657506900,8.0);
            dataPoint.put(1657507200,9.0);
            dataPoint.put(1657507500,10.0);
            dataPoint.put(1657507800,11.0);

            series.setLabels(labels);
            series.setDataPoint(dataPoint);
            seriesSet.add(series);

            //第二条序列
            series = new Series();
            labels = new TreeMap<>();
            labels.put("instance","$instance");
            labels.put("topic","$topic");
            labels.put("consumergroup","$consumergroup02");

            dataPoint = new TreeMap<>();
            dataPoint.put(1657504800,1.0);
            dataPoint.put(1657505100,2.0);
            dataPoint.put(1657505400,3.0);
            dataPoint.put(1657505700,4.0);
            dataPoint.put(1657506000,5.0);
            dataPoint.put(1657506300,6.0);
            dataPoint.put(1657506600,7.0);
            dataPoint.put(1657506900,8.0);
            dataPoint.put(1657507200,9.0);
            dataPoint.put(1657507500,10.0);
            dataPoint.put(1657507800,11.0);

            series.setLabels(labels);
            series.setDataPoint(dataPoint);
            seriesSet.add(series);


            //第三条序列
            series = new Series();
            labels = new TreeMap<>();
            labels.put("instance","$instance");
            labels.put("topic","$topic");
            labels.put("consumergroup","$consumergroup03");

            dataPoint = new TreeMap<>();
            dataPoint.put(1657504800,1.0);
            dataPoint.put(1657505100,2.0);
            dataPoint.put(1657505400,3.0);
            dataPoint.put(1657505700,4.0);
            dataPoint.put(1657506000,5.0);
            dataPoint.put(1657506300,6.0);
            dataPoint.put(1657506600,7.0);
            dataPoint.put(1657506900,8.0);
            dataPoint.put(1657507200,9.0);
            dataPoint.put(1657507500,10.0);
            dataPoint.put(1657507800,11.0);

            series.setLabels(labels);
            series.setDataPoint(dataPoint);
            seriesSet.add(series);

            return seriesSetOperator;
        });
        executorService.execute(futureTask);
    }*/
}
