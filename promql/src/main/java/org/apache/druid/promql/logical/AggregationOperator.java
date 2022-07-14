package org.apache.druid.promql.logical;

import org.apache.druid.promql.data.Series;

import java.util.ArrayList;
import java.util.List;

public abstract class AggregationOperator implements Operator{
    protected List<String> groubBys = new ArrayList<>();
    protected List<Series> seriesSet = new ArrayList<>();

    public List<String> getGroubBys() {
        return groubBys;
    }

    public void setGroubBys(List<String> groubBys) {
        this.groubBys = groubBys;
    }

    public List<Series> getSeriesSet() {
        return seriesSet;
    }

    public void setSeriesSet(List<Series> seriesSet) {
        this.seriesSet = seriesSet;
    }
}
