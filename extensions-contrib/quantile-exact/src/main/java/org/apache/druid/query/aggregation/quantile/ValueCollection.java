package org.apache.druid.query.aggregation.quantile;
import org.apache.druid.query.aggregation.encdec.EncoderDecoder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ValueCollection implements Serializable {

    private Map<Integer,Integer> values = new ConcurrentHashMap<>();

    public List<Integer> sortKey(){
        List<Integer> list = new ArrayList<>(values.keySet());
        Collections.sort(list);
        return list;
    }

    public Integer get(int idx,List<Integer> list){
        int v = 0;
        for(Integer k:list){
            if(v <= idx && idx <= v+values.get(k)){
                return k;
            }
            v += values.get(k);
        }
        return list.get(idx);
    }

    public Integer max(){
        List<Integer> list = new ArrayList<>(values.keySet());
        Collections.sort(list);
        return list.get(list.size()-1);
    }

    public Integer min(){
        List<Integer> list = new ArrayList<>(values.keySet());
        Collections.sort(list);
        return list.get(0);
    }

    public int count(){
        return values.values().stream().mapToInt(Integer::intValue).sum();
    }

    public Double mean(){
        List<Integer> list = new ArrayList<>(values.keySet());
        BigDecimal sum = new BigDecimal(0);
        long count = 0;
        for(Integer k:list){
            sum = sum.add(BigDecimal.valueOf(k*values.get(k)));
            count += values.get(k);
        }
        return sum.doubleValue()/count;
    }

    public void add(Integer k,Integer c){
        if(k < 0){
            k = 0;
        }
        values.put(k.intValue(),values.getOrDefault(k.intValue(),0)+c);
    }

    public ArrayList<Integer> getValues() {
        ArrayList<Integer> vs = new ArrayList<>();
        for(Map.Entry<Integer,Integer> e:values.entrySet()){
            vs.add(e.getKey());
            vs.add(e.getValue());
        }
        return vs;
    }

    public int size(){
        //一半k,一半v
        return this.values.size()*2;
    }

    public void addAll(ValueCollection other){
        for(Map.Entry<Integer,Integer> e:other.values.entrySet()){
            this.values.put(e.getKey(),e.getValue()+this.values.getOrDefault(e.getKey(),0));
        }

    }

    public static  ValueCollection deserialize(byte[] datas){
        return deserialize(ByteBuffer.wrap(datas));
    }

    public static  ValueCollection deserialize(ByteBuffer buffer){
        IntBuffer ib = IntBuffer.allocate(buffer.limit()/Integer.BYTES);
        while(buffer.hasRemaining()){
            ib.put(buffer.getInt());
        }
        int []datas = EncoderDecoder.intDeCompressed(ib.array());
        ValueCollection values = new ValueCollection();
        for(int i = 0; i< datas.length; i+=2){
            values.add(datas[i],datas[i+1]);
        }
        return values;
    }

    public byte[] serialize()
    {
        IntBuffer ib = IntBuffer.allocate(this.size());//内存池优化
        for(Map.Entry<Integer,Integer> e:values.entrySet()){
            ib.put(e.getKey());
            ib.put(e.getValue());
        }
        int [] compressDatas = EncoderDecoder.intCompressed(ib.array());
        //释放内存池内存
        ByteBuffer buffer = ByteBuffer.allocate(compressDatas.length*Integer.BYTES);
        for(int d : compressDatas){
            buffer.putInt(d);
        }
        return buffer.array();
    }
}
