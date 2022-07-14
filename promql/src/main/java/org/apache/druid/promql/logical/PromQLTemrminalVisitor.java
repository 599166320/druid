package org.apache.druid.promql.logical;
import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.antlr.PromQLParserBaseVisitor;
import java.util.*;
public class PromQLTemrminalVisitor extends PromQLParserBaseVisitor<Map<String,String>> {

    private long start;
    private long end;
    private Set<String> publicFields;
    private Map<String,String> sqlMap = new HashMap<>();

    public PromQLTemrminalVisitor(long start, long end, Set<String> publicFields) {
        this.start = start;
        this.end = end;
        this.publicFields = publicFields;
    }

    @Override
    public Map<String, String> visitExpression(PromQLParser.ExpressionContext ctx) {
        super.visitExpression(ctx);
        return sqlMap;
    }

    @Override
    public Map<String, String> visitInstantSelector(PromQLParser.InstantSelectorContext ctx) {
        String metric = ctx.METRIC_NAME().getSymbol().getText();
        StringBuilder where = new StringBuilder();
        TreeSet<String> keySet = new TreeSet<>();
        keySet.add("name="+metric);
        if(ctx.labelMatcherList() != null){
            List<PromQLParser.LabelMatcherContext> labelMatcherContexts =  ctx.labelMatcherList().labelMatcher();
            for(PromQLParser.LabelMatcherContext labelMatcherContext:labelMatcherContexts){
                if(where.length() > 0){
                    where = where.append(" and ");
                }
                String value = labelMatcherContext.STRING().getText().replaceAll("\"","");
                if(publicFields.contains(labelMatcherContext.labelName().getText())){
                    where = where.append(labelMatcherContext.labelName().getText()).append(labelMatcherContext.labelMatcherOperator().getText()).append("'").append(value).append("'");
                }else{
                    where = where.append(" MV_CONTAINS(keys,'").append(labelMatcherContext.labelName().getText()).append("')")
                            .append(" AND MV_CONTAINS(labels,'").append(labelMatcherContext.labelName().getText()).append("=").append(value).append("')");
                }
                keySet.add(labelMatcherContext.labelName().getText()+","+labelMatcherContext.labelMatcherOperator().getText()+","+labelMatcherContext.STRING().getText());
            }
        }
        String sql;
        if(where.length() > 0){
            sql = "select * from \"middleware-monitor-metrics-test01\" where name='"+metric+"' and __time>=MILLIS_TO_TIMESTAMP("+start+") and __time<=MILLIS_TO_TIMESTAMP("+end+") and "+where;
        }else {
            sql = "select * from \"middleware-monitor-metrics-test01\" where name='"+metric+"' and __time>=MILLIS_TO_TIMESTAMP("+start+") and __time<=MILLIS_TO_TIMESTAMP("+end+")";
        }

        String key = String.join(",",keySet);

        sqlMap.put(key,sql);
        return sqlMap;
    }
}
