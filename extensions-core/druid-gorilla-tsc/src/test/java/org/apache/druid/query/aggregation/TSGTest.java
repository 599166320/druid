package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.core.*;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

public class TSGTest {

    @Test
    public void windows(){

        Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> maxFun = (list)->{
            Map.Entry<Long, Double> max = list.getFirst();
            for(Map.Entry<Long, Double> e : list){
                if(max.getValue() < e.getValue()){
                    max = e;
                }
            }
            return max;
        };

        Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> minFun = (list)->{
            Map.Entry<Long, Double> min = list.getFirst();
            for(Map.Entry<Long, Double> e : list){
                if(min.getValue() > e.getValue()){
                    min = e;
                }
            }
            return min;
        };

        Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> increaseFun = (list)->{
            Map.Entry<Long, Double> first = list.getFirst();
            Map.Entry<Long, Double> last = list.getLast();

            for(Map.Entry<Long, Double> e: ImmutableMap.of(last.getKey(),last.getValue()-first.getValue()).entrySet()){
                return e;
            }
            return null;
        };

        TreeMap<Long,Double> treeMap = new TreeMap<Long,Double>();
        treeMap.put(1L,1.0);
        treeMap.put(2L,2.0);
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
        treeMap.put(15L,25.0);


        treeMap = TSG.slidingWindow(treeMap,3,1,increaseFun);
        System.out.println(treeMap);
    }

    @Test
    public void test(){

        //TSG tmp = new TSG(100);
        //tmp.put(100,1.2);
        TSG tmp = TSG.fromBytes(StringUtils.decodeBase64String("AAAAAGKUnGIAAAAAYpW1SEHuHwAAAAAAAAAADwAAAAAAAAAAAAAnhWKclGIAAAAAAAAAAAAA4Dl8UA8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPQIAAAAALAUAKA4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA6BEAAAAAAMIHQHEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD0CAAAAAKgKACgOAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAH+5BPQIAAAAA+HAPojg="));
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
