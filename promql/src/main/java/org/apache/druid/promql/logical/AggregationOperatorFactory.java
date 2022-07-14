package org.apache.druid.promql.logical;

public class AggregationOperatorFactory {
    public static AggregationOperator createAggregationOperatorByName(String aggName){
        if("avg".equals(aggName)){
            return new AvgAggregationOperator();
        }else if("sum".equals(aggName)){
            return new SumAggregationOperator();
        }
        return null;
    }
}
