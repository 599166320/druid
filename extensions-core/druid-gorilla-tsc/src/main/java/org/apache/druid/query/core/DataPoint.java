package org.apache.druid.query.core;

public interface DataPoint
{
    long getTime();

    double getValue();
}
