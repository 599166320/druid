package org.apache.druid.promql;
import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.antlr.PromQLParserBaseVisitor;
import org.apache.druid.promql.logical.Operator;
import org.apache.druid.promql.logical.QueryOperator;

public class PromQLVisitor extends PromQLParserBaseVisitor<Operator> {
    private QueryOperator queryOp;

    @Override
    public Operator visitExpression(PromQLParser.ExpressionContext ctx) {
        return super.visitExpression(ctx);
    }
}
