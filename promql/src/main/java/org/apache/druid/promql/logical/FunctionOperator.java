package org.apache.druid.promql.logical;

import java.util.List;

public abstract class FunctionOperator implements Operator {
    protected List<Operator> params;
}
