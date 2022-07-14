package org.apache.druid.promql;

import org.apache.druid.promql.logical.SumAggregationOperator;

public class SumAggregationOperatorTests {
    public void test(){
        SumAggregationOperator sumAggregationOperator = new SumAggregationOperator();
        sumAggregationOperator.call();
    }
}
