package org.apache.druid.promql.logical;
import java.util.*;
import java.util.concurrent.FutureTask;
public class InstantOperator implements Operator{
    //protected String key;
    protected String metric;
    protected List<Map<String,Object>> where;
    protected Map<String, Object> props;
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

    public SeriesSetOperator getSeriesSetOperator() {
        return seriesSetOperator;
    }

    public void setSeriesSetOperator(SeriesSetOperator seriesSetOperator) {
        this.seriesSetOperator = seriesSetOperator;
    }

    @Override
    public Operator call() throws Exception {
        return seriesSetOperator;
    }
}
