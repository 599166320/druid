package org.apache.druid.query.aggregation;

import org.apache.druid.query.core.*;
import org.junit.Test;

import java.util.Iterator;

public class TSGTest {

    @Test
    public void test(){
        TSG tsg = new TSG(1546300800, new OutBitSet());
        tsg.put(1546300800, 4.2);
        tsg.close();
        byte[] tsgBytes = tsg.getDataBytes();
        Iterator<DataPoint> tsgIterator = new TSGIterator(new InBitSet(tsgBytes));
        while (tsgIterator.hasNext()){
            DataPoint p = tsgIterator.next();
            System.out.println(p.getTime()+","+p.getValue());
        }
    }
}
