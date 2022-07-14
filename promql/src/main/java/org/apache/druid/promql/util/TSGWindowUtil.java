package org.apache.druid.promql.util;
import com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.function.Function;

public class TSGWindowUtil {

    public static TreeMap<Integer,Double> slidingWindow(TreeMap<Integer,Double>  treeMap, int windowSize, int slideSize, Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> function) {

        Map.Entry<Integer,Double>[] entrys = new Map.Entry[treeMap.size()];
        int i = 0;
        for(Map.Entry<Integer,Double> entry:treeMap.entrySet()){
            Map.Entry<Integer,Double>[] tmp = new Map.Entry[1];
            ImmutableMap.of(entry.getKey()-entry.getKey()%slideSize,entry.getValue()).entrySet().toArray(tmp);
            entrys[i++] = tmp[0];
        }
        treeMap.clear();

        List<Map.Entry<Integer,Double>> res = new ArrayList<>();
        MonotoneQueue window = new MonotoneQueue(function);

        long ts = entrys[0].getKey()+windowSize;//windowSize单位是s

        for (Map.Entry<Integer, Double> entry : entrys) {
            if (entry.getKey() <= ts) {
                window.push(entry);
            } else {
                res.add(window.apply());
                window.slide(slideSize);
                window.push(entry);
                ts = window.getFist().getKey()+windowSize;
            }
        }

        for (Map.Entry<Integer, Double> re : res) {
            treeMap.put(re.getKey(), re.getValue());
        }

        return treeMap;
    }

    //单调队列
    static class MonotoneQueue{
        LinkedList<Map.Entry<Integer,Double>> queue = new LinkedList<Map.Entry<Integer,Double>>();
        Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> function;
        public MonotoneQueue(Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> function){
            this.function = function;
        }

        public void push(Map.Entry<Integer,Double> n){
            queue.addLast(n);
        }

        public Map.Entry<Integer, Double> apply(){
            return function.apply(queue);
        }

        public void pop(){
            queue.pollFirst();
        }

        public void slide(int slideSize){
            long fistTime = queue.getFirst().getKey() + slideSize;
            int removeSize = 0;
            for (Map.Entry<Integer, Double> longDoubleEntry : queue) {
                if (longDoubleEntry.getKey() < fistTime) {
                    removeSize++;
                }
            }
            int i = 0;
            while (i++ < removeSize){
                pop();
            }
        }

        public Map.Entry<Integer,Double> getFist(){
            return queue.getFirst();
        }
    }



    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> MAX_FUN = (list)->{
        Map.Entry<Integer, Double> max = list.getFirst();
        for(Map.Entry<Integer, Double> e : list){
            if(max.getValue() < e.getValue()){
                max = e;
            }
        }
        return max;
    };

    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> MIN_FUN = (list)->{
        Map.Entry<Integer, Double> min = list.getFirst();
        for(Map.Entry<Integer, Double> e : list){
            if(min.getValue() > e.getValue()){
                min = e;
            }
        }
        return min;
    };

    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> INCREASE_FUN = (list)->{
        Map.Entry<Integer, Double> first = list.getFirst();
        Map.Entry<Integer, Double> last = list.getLast();

        for(Map.Entry<Integer, Double> e: ImmutableMap.of(last.getKey(),last.getValue()-first.getValue()).entrySet()){
            return e;
        }
        return null;
    };


    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> RATE_FUN = (list)->{
        Map.Entry<Integer, Double> first = list.getFirst();
        Map.Entry<Integer, Double> last = list.getLast();

        for(Map.Entry<Integer, Double> e: ImmutableMap.of(last.getKey(),(last.getValue()-first.getValue())/(last.getKey()-first.getKey())).entrySet()){
            return e;
        }
        return null;
    };

    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> LAST_FUN = (list)->{
        Map.Entry<Integer, Double> last = list.getLast();
        return last;
    };

    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> FIRST_FUN = (list)->{
        Map.Entry<Integer, Double> first = list.getFirst();
        return first;
    };

    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> DELTA_FUN = (list)->{
        Map.Entry<Integer, Double> first = list.getFirst();
        Map.Entry<Integer, Double> last = list.getLast();

        for(Map.Entry<Integer, Double> e: ImmutableMap.of(last.getKey(),last.getValue()>first.getValue()?last.getValue()-first.getValue():first.getValue()-last.getValue()).entrySet()){
            return e;
        }
        return null;
    };

    public static final Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> IDELTA_FUN = (list)->{
        Map.Entry<Integer, Double> last = list.getLast();
        Map.Entry<Integer, Double> first = list.get(list.size()-2);
        for(Map.Entry<Integer, Double> e: ImmutableMap.of(first.getKey(),last.getValue()>first.getValue()?last.getValue()-first.getValue():first.getValue()-last.getValue()).entrySet()){
            return e;
        }
        return null;
    };


    public static final Map<String,Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>>> FUN_MAP = new HashMap<>();
    static {
        FUN_MAP.put("max",MAX_FUN);
        FUN_MAP.put("min",MIN_FUN);
        FUN_MAP.put("increase",INCREASE_FUN);
        FUN_MAP.put("rate",RATE_FUN);
        FUN_MAP.put("delta",DELTA_FUN);
        FUN_MAP.put("idelta",IDELTA_FUN);
        FUN_MAP.put("last",LAST_FUN);
        FUN_MAP.put("first",FIRST_FUN);
    }

    public static Function<LinkedList<Map.Entry<Integer,Double>>,Map.Entry<Integer, Double>> getFun(String fun){
        return FUN_MAP.get(fun);
    }
}
