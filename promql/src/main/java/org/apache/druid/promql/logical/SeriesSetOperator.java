package org.apache.druid.promql.logical;
import org.apache.druid.promql.data.Series;

import java.util.ArrayList;
import java.util.List;

public class SeriesSetOperator implements Operator{
    private List<Series> seriesSet = new ArrayList<>();

    public List<Series> getSeriesSet() {
        return seriesSet;
    }

    public void setSeriesSet(List<Series> seriesSet) {
        this.seriesSet = seriesSet;
    }

    @Override
    public Operator call() throws Exception {
        return this;
    }
}
