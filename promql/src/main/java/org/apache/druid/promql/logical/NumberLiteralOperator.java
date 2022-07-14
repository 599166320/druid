package org.apache.druid.promql.logical;

public class NumberLiteralOperator implements Operator {

    private Number number;

    public NumberLiteralOperator(Number number) {
        this.number = number;
    }

    @Override
    public Operator call() throws Exception {
        return this;
    }

    public Number getNumber() {
        return number;
    }

    public void setNumber(Number number) {
        this.number = number;
    }
}
