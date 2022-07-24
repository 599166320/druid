package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.druid.query.aggregation.encdec.EncoderDecoder;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
public class ValueCollection implements Serializable {

    private Map<Integer,Integer> values = new HashMap<>();

    public Integer get(int idx){
        List<Integer> list = new ArrayList<>(values.keySet());
        Collections.sort(list);
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
            values.put(e.getKey(),e.getValue()+values.getOrDefault(e.getKey(),0));
        }
    }

    public static  ValueCollection deserialize(byte[] datas){
        return deserialize(ByteBuffer.wrap(datas));
    }

    public static  ValueCollection deserialize(ByteBuffer buffer){
        //int srcLen = buffer.getInt();
        //length太大
        //datas = EncoderDecoder.decompressorByte(buffer.get(new byte[buffer.limit()-buffer.position()],0,buffer.limit()-buffer.position()).array(),srcLen);
        ValueCollection values = new ValueCollection();
        while(buffer.hasRemaining()){
            values.add(buffer.getInt(),buffer.getInt());
        }
        return values;
    }

    public byte[] serialize()
    {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES*this.size());
        ArrayList<Integer> vlist = this.getValues();
        for(Object v:vlist){
            buffer.putInt((Integer) v);
        }
        return buffer.array();
        /*
        byte [] datas = buffer.array();
        int srcLen = datas.length;
        byte[] compressedBytes = EncoderDecoder.compressedByte(datas);
        buffer = ByteBuffer.allocate(Integer.BYTES+compressedBytes.length);
        buffer.putInt(srcLen);
        buffer.put(compressedBytes);
        return buffer.array();*/
    }

}
