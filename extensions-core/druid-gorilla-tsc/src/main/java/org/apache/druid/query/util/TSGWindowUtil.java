package org.apache.druid.query.util;

import com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.function.Function;

public class TSGWindowUtil {

    public static TreeMap<Long,Double> slidingWindow(TreeMap<Long,Double>  treeMap, int windowSize, int slideSize, Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> function) {

        Map.Entry<Long,Double>[] entrys = new Map.Entry[treeMap.size()];
        treeMap.entrySet().toArray(entrys);
        treeMap.clear();


        List<Map.Entry<Long,Double>> res = new ArrayList<>();
        MonotoneQueue window = new MonotoneQueue(function);

        long ts = entrys[0].getKey()+windowSize;//windowSize单位是s

        for (Map.Entry<Long, Double> entry : entrys) {
            if (entry.getKey() < ts) {
                window.push(entry);
            } else {
                res.add(window.apply());
                window.slide(slideSize);
                window.push(entry);
                ts = window.getFist().getKey()+windowSize;
            }
        }

        for (Map.Entry<Long, Double> re : res) {
            treeMap.put(re.getKey(), re.getValue());
        }

        return treeMap;
    }

    //单调队列
    static class MonotoneQueue{
        LinkedList<Map.Entry<Long,Double>> queue = new LinkedList<Map.Entry<Long,Double>>();
        Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> function;
        public MonotoneQueue(Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> function){
            this.function = function;
        }

        public void push(Map.Entry<Long,Double> n){
            queue.addLast(n);
        }

        public Map.Entry<Long, Double> apply(){
            return function.apply(queue);
        }

        public void pop(){
            queue.pollFirst();
        }

        public void slide(int slideSize){
            long fistTime = queue.getFirst().getKey() + slideSize;
            int removeSize = 0;
            for (Map.Entry<Long, Double> longDoubleEntry : queue) {
                if (longDoubleEntry.getKey() < fistTime) {
                    removeSize++;
                }
            }
            int i = 0;
            while (i++ < removeSize){
                pop();
            }
        }

        public Map.Entry<Long,Double> getFist(){
            return queue.getFirst();
        }
    }



    public static final Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> MAX_FUN = (list)->{
        Map.Entry<Long, Double> max = list.getFirst();
        for(Map.Entry<Long, Double> e : list){
            if(max.getValue() < e.getValue()){
                max = e;
            }
        }
        return max;
    };

    public static final Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> MIN_FUN = (list)->{
        Map.Entry<Long, Double> min = list.getFirst();
        for(Map.Entry<Long, Double> e : list){
            if(min.getValue() > e.getValue()){
                min = e;
            }
        }
        return min;
    };

    public static final Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> INCREASE_FUN = (list)->{
        Map.Entry<Long, Double> first = list.getFirst();
        Map.Entry<Long, Double> last = list.getLast();

        for(Map.Entry<Long, Double> e: ImmutableMap.of(last.getKey(),last.getValue()-first.getValue()).entrySet()){
            return e;
        }
        return null;
    };


    public static final Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> RATE_FUN = (list)->{
        Map.Entry<Long, Double> first = list.getFirst();
        Map.Entry<Long, Double> last = list.getLast();

        for(Map.Entry<Long, Double> e: ImmutableMap.of(last.getKey(),(last.getValue()-first.getValue())/(last.getKey()-first.getKey())).entrySet()){
            return e;
        }
        return null;
    };

    public static final Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> DELTA_FUN = (list)->{
        Map.Entry<Long, Double> first = list.getFirst();
        Map.Entry<Long, Double> last = list.getLast();

        for(Map.Entry<Long, Double> e: ImmutableMap.of(last.getKey(),last.getValue()>first.getValue()?last.getValue()-first.getValue():first.getValue()-last.getValue()).entrySet()){
            return e;
        }
        return null;
    };

    public static final Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> IDELTA_FUN = (list)->{
        Map.Entry<Long, Double> last = list.getLast();
        Map.Entry<Long, Double> first = list.get(list.size()-2);
        for(Map.Entry<Long, Double> e: ImmutableMap.of(first.getKey(),last.getValue()>first.getValue()?last.getValue()-first.getValue():first.getValue()-last.getValue()).entrySet()){
            return e;
        }
        return null;
    };

    public static final Map<String,Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>>> FUN_MAP = new HashMap<>();
    static {
        FUN_MAP.put("max",MAX_FUN);
        FUN_MAP.put("min",MIN_FUN);
        FUN_MAP.put("increase",INCREASE_FUN);
        FUN_MAP.put("rate",RATE_FUN);
        FUN_MAP.put("delta",DELTA_FUN);
        FUN_MAP.put("idelta",IDELTA_FUN);
    }

    public static Function<LinkedList<Map.Entry<Long,Double>>,Map.Entry<Long, Double>> getFun(String fun){
        return FUN_MAP.get(fun);
    }

}
