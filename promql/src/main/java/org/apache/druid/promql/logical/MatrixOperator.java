package org.apache.druid.promql.logical;

public class MatrixOperator extends InstantOperator{
    protected int range;

    public int getRange() {
        return range;
    }

    public void setRange(int range) {
        this.range = range;
    }

}
