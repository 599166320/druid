package org.apache.druid.indexing.common.config;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class OtherConfig extends Properties {
    @Override
    public  String toString() {
        Set<Map.Entry<Object,Object>> entrySet =  this.entrySet();
        StringBuffer stringBuffer = new StringBuffer("OtherConfig={");
        for(Map.Entry<Object,Object> entry:entrySet){
            stringBuffer.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
        }
        return stringBuffer.append("}").toString();
    }
}
