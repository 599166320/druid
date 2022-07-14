package org.apache.druid.promql.logical;

public class BooleanOperator implements Operator{

    private boolean value;

    public BooleanOperator(boolean value) {
        this.value = value;
    }

    public boolean getValue() {
        return value;
    }

    public void setValue(boolean value) {
        this.value = value;
    }

    public boolean isValue() {
        return value;
    }

    @Override
    public Operator call() throws Exception {
        return this;
    }

}
