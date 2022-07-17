package org.apache.druid.promql.logical;
import java.io.Serializable;
import java.util.concurrent.Callable;

public interface  Operator extends Callable<Operator>, Serializable {

}
