package org.apache.druid.promql.logical;

import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.antlr.PromQLParserBaseVisitor;

public class RangeVisitor extends PromQLParserBaseVisitor<Integer> {

    int range;

    @Override
    public Integer visitExpression(PromQLParser.ExpressionContext ctx) {
        super.visitExpression(ctx);
        return range;
    }

    @Override
    public Integer visitMatrixSelector(PromQLParser.MatrixSelectorContext ctx) {
        String rangeText = ctx.TIME_RANGE().getText().replaceAll("\\[","").replaceAll("]","");
        if(rangeText.endsWith("s")){
            range = Integer.parseInt(rangeText.replaceAll("s",""));
        }else {
            range = Integer.parseInt(rangeText.replaceAll("m","")) * 60;
        }
        return range;
    }
}
