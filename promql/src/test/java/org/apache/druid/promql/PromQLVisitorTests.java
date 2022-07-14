package org.apache.druid.promql;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.apache.druid.promql.antlr.PromQLLexer;
import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.data.Series;
import org.apache.druid.promql.logical.Operator;
import org.apache.druid.promql.logical.PromQLTemrminalVisitor;
import org.apache.druid.promql.logical.SeriesSetOperator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PromQLVisitorTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(PromQLVisitorTests.class);

    @Test
    public void test() throws IOException {
        //kafka_consumergroup_lag{instance="$instance",topic=~"$topic",consumergroup=~"$consumer_group"}
        //sum(kafka_consumergroup_lag{instance="$instance",topic=~"$topic",consumergroup=~"$consumer_group"}) by (consumergroup, topic)
        //sum(rate(kafka_topic_partition_current_offset{instance="$instance",topic=~"$topic"}[1m]))
        //sum(increase(kafka_topic_partition_current_offset{instance="$instance",topic=~"$topic"}[1m]))
        //avg(increase(kafka_topic_partition_current_offset{instance="$instance",topic=~"$topic"}[1m]))
        //histogram_quantile(0.99, sum(increase(kafka_topic_partition_current_offset{}[15s])) by (le,instance))
        //sum(kafka_consumergroup_lag{instance="$instance",topic=~"$topic",consumergroup=~"$consumer_group",le="0"}) by (consumergroup, topic,le)
        //String promql = "histogram_quantile(0.99, sum(increase(kafka_topic_partition_current_offset{}[15s])) by (le,instance))";
        String promql = "sum(kafka_consumergroup_lag{instance=\"$instance\",topic=~\"$topic\",consumergroup=~\"$consumer_group\",le=\"0\"}) by (consumergroup, topic,le)";
        PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(promql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        PromQLParser parser = new PromQLParser(tokenStream);
        PromQLParser.ExpressionContext expressionContext = parser.expression();

        PromQLTemrminalVisitor promQLTemrminalVisitor = new PromQLTemrminalVisitor(0,1, ImmutableSet.of("name","env","app","clusterName","le"));
        Map<String,String> sqlMap =  promQLTemrminalVisitor.visitExpression(expressionContext);
        System.out.println(MAPPER.writeValueAsString(sqlMap));
        //查询结果放在缓存中
        Map<String, SeriesSetOperator> seriesSetOperatorMap = new HashMap<>();

        for(Map.Entry<String,String> e:sqlMap.entrySet()){
            seriesSetOperatorMap.put(e.getKey(),getSeriesSetOperator());
        }

        System.out.println(expressionContext.toStringTree(parser));
        PromQLVisitor visitor = new PromQLVisitor();
        visitor.setRawSeriesSetOperatorMap(seriesSetOperatorMap);
        visitor.setStep(15);
        Operator result = visitor.visitExpression(expressionContext);
        System.out.println(MAPPER.writeValueAsString(result));
    }


    public SeriesSetOperator getSeriesSetOperator(){
        //执行sql查询
        SeriesSetOperator seriesSetOperator = new SeriesSetOperator();
        List<Series> seriesSet = new ArrayList<>();
        seriesSetOperator.setSeriesSet(seriesSet);
        Map<String,Series> seriesMap = new HashMap<>();

        double i = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/gz00018ml/myproject/druid/promql/src/main/resources/quantile.json")))){
            String line;
            while ((line = br.readLine()) != null){
                JSONObject obj = JSON.parseObject(line);
                String instance = obj.getString("instance");
                String topic = obj.getString("topic");
                String consumergroup = obj.getString("consumergroup");
                String le = obj.getString("le");
                int time = obj.getIntValue("time");
                double value = obj.getDoubleValue("value");
                value = ((i++) / 32)*value+value;

                String  k = instance+","+topic+","+consumergroup+","+le;
                if(seriesMap.containsKey(k)){
                    Series series = seriesMap.get(k);
                    series.getDataPoint().put(time,value);
                }else {
                    TreeMap<String,Object> lb = new TreeMap<>();
                    lb.put("instance",instance);
                    lb.put("topic",topic);
                    lb.put("consumergroup",consumergroup);
                    lb.put("le",le);

                    TreeMap<Integer,Double> dp = new TreeMap<>();
                    dp.put(time,value);
                    Series series = new Series();
                    series.setLabels(lb);
                    series.setDataPoint(dp);
                    seriesSet.add(series);
                    seriesMap.put(k,series);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return seriesSetOperator;
    }
}
