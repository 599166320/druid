package org.apache.druid.query.aggregation.histogram.expression;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogram;
import org.apache.druid.segment.column.ValueType;
import javax.annotation.Nullable;
import java.util.List;
public class QuantileExtractExprMacro implements ExprMacroTable.ExprMacro{

    private static final Logger log = new Logger(QuantileExtractExprMacro.class);
    public static final String FN_NAME = "quantile_extract";

    @Override
    public String name() {
        return FN_NAME;
    }

    @Override
    public Expr apply(List<Expr> args) {
        if (args.size() != 2) {
            throw new IAE("Function[%s] must have 2 arguments", name());
        }
        final Expr columExpr = args.get(0);
        final Expr funExpr = args.get(1);
        String fun = (String) funExpr.getLiteralValue();
        class  QuantileExtractExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr{

            private QuantileExtractExpr(Expr arg) {
                super(FN_NAME, arg);
            }

            @Override
            public ExprEval eval(ObjectBinding bindings) {
                Object val = arg.eval(bindings).value();
                if (val == null) {
                    return ExprEval.of(null);
                }
                if(val instanceof String){
                    val = FixedBucketsHistogram.fromBase64((String) val);
                }
                FixedBucketsHistogram fixedBucketsHistogram = (FixedBucketsHistogram) val;
                Double[] percentiles = fixedBucketsHistogram.percentilesDouble(new double[]{50.0, 95.0, 99.0});
                return ExprEval.ofDoubleArray(percentiles);
            }

            @Override
            public Expr visit(Shuttle shuttle) {
                Expr newArg = arg.visit(shuttle);
                return shuttle.visit(new QuantileExtractExpr(newArg));
            }

            @Nullable
            @Override
            public ExprType getOutputType(InputBindingInspector inspector)
            {
                return ExprType.fromValueType(ValueType.DOUBLE_ARRAY);
            }
        }
        return new QuantileExtractExpr(columExpr);
    }
}
