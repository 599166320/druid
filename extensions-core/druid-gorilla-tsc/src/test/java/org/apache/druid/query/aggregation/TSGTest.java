package org.apache.druid.query.aggregation;

import org.apache.druid.query.core.*;
import org.junit.Test;

import java.util.Iterator;

public class TSGTest {

    @Test
    public void test(){
        TSG tsg = new TSG(1650844800, new OutBitSet());
        tsg.put(1650844800,1000.0);
        tsg.put(1650844860,1001.0);
        tsg.put(1650844920,1003.0);
        tsg.put(1650844980,1004.0);
        tsg.put(1650845040,1004.0);
        tsg.put(1650845100,1005.0);
        tsg.put(1650845160,1006.0);
        tsg.put(1650845220,1007.0);
        tsg.put(1650845280,1008.0);
        tsg.put(1650848880,1008.0);
        tsg.close();
        byte[] tsgBytes = tsg.getDataBytes();
        Iterator<DataPoint> tsgIterator = new TSGIterator(new InBitSet(tsgBytes));
        while (tsgIterator.hasNext()){
            DataPoint p = tsgIterator.next();
            System.out.println(p.getTime()+","+p.getValue());
        }
    }
}
