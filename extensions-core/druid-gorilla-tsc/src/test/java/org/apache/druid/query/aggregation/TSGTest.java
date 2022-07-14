package org.apache.druid.query.aggregation;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.util.TSGWindowUtil;
import org.apache.druid.tsg.DataPoint;
import org.apache.druid.tsg.TSG;
import org.junit.Test;
import java.util.*;
public class TSGTest {

    @Test
    public void windows(){

        TreeMap<Long,Double> treeMap;
        treeMap = TSGWindowUtil.slidingWindow(getTreeMap(),3,1,TSGWindowUtil.MIN_FUN);
        System.out.println("max:"+treeMap);

        treeMap = TSGWindowUtil.slidingWindow(getTreeMap(),3,1,TSGWindowUtil.MIN_FUN);
        System.out.println("min:"+treeMap);

        treeMap = TSGWindowUtil.slidingWindow(getTreeMap(),3,1,TSGWindowUtil.INCREASE_FUN);
        System.out.println("increase:"+treeMap);

        treeMap = TSGWindowUtil.slidingWindow(getTreeMap(),3,1,TSGWindowUtil.RATE_FUN);
        System.out.println("rate:"+treeMap);

        treeMap = TSGWindowUtil.slidingWindow(getTreeMap(),3,1,TSGWindowUtil.DELTA_FUN);
        System.out.println("delta:"+treeMap);

        treeMap = TSGWindowUtil.slidingWindow(getTreeMap(),3,1,TSGWindowUtil.IDELTA_FUN);
        System.out.println("idelta:"+treeMap);

    }


    private TreeMap<Long,Double> getTreeMap(){
        TreeMap<Long,Double> treeMap = new TreeMap<Long,Double>();
        treeMap.put(1L,1.0);
        /*treeMap.put(2L,2.0);
        treeMap.put(3L,5.0);
        treeMap.put(4L,8.0);
        treeMap.put(5L,9.0);
        treeMap.put(6L,12.0);
        treeMap.put(7L,16.0);
        treeMap.put(8L,17.0);
        treeMap.put(9L,19.0);
        treeMap.put(10L,20.0);
        treeMap.put(11L,21.0);
        treeMap.put(12L,22.0);
        treeMap.put(13L,23.0);
        treeMap.put(14L,24.0);
        treeMap.put(15L,25.0);*/
        return treeMap;
    }

    @Test
    public void test(){

        //TSG tmp = new TSG(100);
        //tmp.put(100,1.2);
        String str = "AAAAAGLPzQAAAAAAYs/YvT+vUjD6SNvnAAAADwAAAAAAAAAAAAABggDNz2IAAAAAckvg5lIx1Vnrz+/S//9HDG32y+oyAJA4ws/coTJ8DwCZPHAPt45/";
        //TSG tmp = TSG.fromBytes(StringUtils.decodeBase64String("AAAAAGKUnFMAAAAAYpW1OQAAAAAAAAAAAAAADwAAAAAAAAAAAAApV1OclGIAAAAAAAAAAAAAAAD8Tw8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAgP8RAAAAAAAA/A8AAAgAAAAAAAD+BwEAAAAAAMD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAID/ARAAAAAAAAD8DwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAC9k7PTwAAABCwd3p+AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAACA/xEAAAAAAAD8DwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAOB/EAAAAAAAAPyPAAAAAAAA4H8="));
        TSG tmp = TSG.fromBytes(StringUtils.decodeBase64String(str));
        /*long a = tmp.getStartTime();
        long b = tmp.getTime();
        System.out.println(a+","+b);
        System.out.println(tmp.getOutBitSet().getSize());*/
        Iterator<DataPoint> tsgIterator1 = tmp.toIterator();
        while (tsgIterator1.hasNext()){
            DataPoint p = tsgIterator1.next();
            System.out.println(p.getTime()+","+p.getValue());
        }

        /*
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
        }*/
    }
}
