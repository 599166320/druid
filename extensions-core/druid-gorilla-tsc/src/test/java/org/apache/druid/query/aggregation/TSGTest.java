package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.core.*;
import org.junit.Test;

import java.util.Iterator;

public class TSGTest {

    @Test
    public void test(){

        //TSG tmp = new TSG(100);
        //tmp.put(100,1.2);
        TSG tmp = TSG.fromBytes(StringUtils.decodeBase64String("AAAAAGKGFDUAAAAAYoYUrT/wAAAAAAAAAAAADwAAAAAAAAAAAAAApjUUhmIAAAAAAAAAAAAAAAD8Tw8="));
        /*long a = tmp.getStartTime();
        long b = tmp.getTime();
        System.out.println(a+","+b);
        System.out.println(tmp.getOutBitSet().getSize());*/
        Iterator<DataPoint> tsgIterator1 = tmp.toIterator();
        while (tsgIterator1.hasNext()){
            DataPoint p = tsgIterator1.next();
            System.out.println(p.getTime()+","+p.getValue());
        }


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
