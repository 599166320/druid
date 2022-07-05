package org.apache.druid.tsg;

public interface DataPoint
{
    long getTime();

    double getValue();
}
