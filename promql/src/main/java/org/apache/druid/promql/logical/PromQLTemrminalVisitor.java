package org.apache.druid.promql.logical;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.antlr.PromQLParserBaseVisitor;
import java.util.*;
public class PromQLTemrminalVisitor extends PromQLParserBaseVisitor<Map<String,String>> {

    private long start;
    private long end;
    private Map<String,String> sqlMap = new HashMap<>();
    public static final String  KEYS_FIELDS = "keys";
    public static final Set<String>  SERIES_FIELDS = ImmutableSet.of("name","env","app","clusterName","ip","bu","instance","p","port","quantile","labels","le");
    public static final Set<String> PUBLIC_FIELDS = ImmutableSet.of("name","env","app","clusterName","ip","bu","instance","p","port","quantile",KEYS_FIELDS,"labels","\"value\"","le");
    public static final Set<String> PUBLIC_FIELDS_NO_KEYS = ImmutableSet.of("name","env","app","clusterName","ip","bu","instance","p","port","quantile","labels","le");
    public static final int LIMIT  = 10000;
    public static final String TABLE = "\"middleware-monitor-metrics-test01\"";


    public PromQLTemrminalVisitor(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public Map<String, String> visitExpression(PromQLParser.ExpressionContext ctx) {
        super.visitExpression(ctx);
        return sqlMap;
    }

    @Override
    public Map<String, String> visitInstantSelector(PromQLParser.InstantSelectorContext ctx) {
        StringBuilder where = whereBuilder(ctx);
        TreeSet<String> keySet = new TreeSet<>();
        if(ctx.METRIC_NAME() != null){
            String metric = ctx.METRIC_NAME().getSymbol().getText();
            keySet.add("name="+metric);
        }
        if(ctx.labelMatcherList() != null){
            List<PromQLParser.LabelMatcherContext> labelMatcherContexts =  ctx.labelMatcherList().labelMatcher();
            for(PromQLParser.LabelMatcherContext labelMatcherContext:labelMatcherContexts){
                if("__name__".equals(labelMatcherContext.labelName().getText())){
                    keySet.add("name,"+labelMatcherContext.labelMatcherOperator().getText()+","+labelMatcherContext.STRING().getText());
                }else{
                    keySet.add(labelMatcherContext.labelName().getText()+","+labelMatcherContext.labelMatcherOperator().getText()+","+labelMatcherContext.STRING().getText());
                }
            }
        }
        String sql;

        //long __time_start = start-start%3600_000;
        //long __time_end = end-end%3600_000;

        long __time_start = start-3600_000;
        long __time_end = end;

        sql = "select "+queryFieldsNoKeysStr()+",\"value\"  from "+TABLE+" where  __time>=MILLIS_TO_TIMESTAMP("+__time_start+") and __time<=MILLIS_TO_TIMESTAMP("+__time_end+") and "+where;

        String key = String.join(",",keySet);
        sqlMap.put(key,sql);
        return sqlMap;
    }

    private static StringBuilder whereBuilder(PromQLParser.InstantSelectorContext ctx){
        StringBuilder where = new StringBuilder();
        if(ctx.METRIC_NAME() !=  null ){
            String metric = ctx.METRIC_NAME().getSymbol().getText();
            where = where.append(" name=").append("'").append(metric).append("'");
        }
        if(ctx.labelMatcherList() != null){
            List<PromQLParser.LabelMatcherContext> labelMatcherContexts =  ctx.labelMatcherList().labelMatcher();
            for(PromQLParser.LabelMatcherContext labelMatcherContext:labelMatcherContexts){
                if(where.length() > 0){
                    where = where.append(" and ");
                }
                String value = labelMatcherContext.STRING().getText().replaceAll("\"","");
                if(PUBLIC_FIELDS.contains(labelMatcherContext.labelName().getText())){
                    where = where.append(labelMatcherContext.labelName().getText()).append(labelMatcherContext.labelMatcherOperator().getText()).append("'").append(value).append("'");
                }else if("__name__".equals(labelMatcherContext.labelName().getText())){
                    where = where.append("name").append(labelMatcherContext.labelMatcherOperator().getText()).append("'").append(value).append("'");
                }else{
                    where = where.append(" MV_CONTAINS(keys,'").append(labelMatcherContext.labelName().getText()).append("')")
                            .append(" AND MV_CONTAINS(labels,'").append(labelMatcherContext.labelName().getText()).append("=").append(value).append("')");
                }
            }
        }
        return where;
    }

    public static String queryFieldsNoKeysStr(){
        return String.join(",",PUBLIC_FIELDS_NO_KEYS);
    }

    public static class LabelMatcherVisitor extends PromQLParserBaseVisitor<String>{
        private String where;
        @Override
        public String visitExpression(PromQLParser.ExpressionContext ctx) {
            super.visitExpression(ctx);
            return where;
        }

        @Override
        public String visitVectorOperation(PromQLParser.VectorOperationContext ctx) {
            if(ctx.vectorOperation() != null && ctx.vectorOperation().size() > 1){
                throw new RuntimeException("It must be a single metric.");
            }
            return super.visitVectorOperation(ctx);
        }

        @Override
        public String visitInstantSelector(PromQLParser.InstantSelectorContext ctx) {
            where = whereBuilder(ctx).toString();
            return super.visitInstantSelector(ctx);
        }
    }
}
