package org.apache.druid.query.aggregation.expression;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.core.DataPoint;
import org.apache.druid.query.core.TSG;
import org.apache.druid.query.core.TSGIterator;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
public class GorillaTimeSeriesExprMacro implements ExprMacroTable.ExprMacro{
    private static final Logger log = new Logger(GorillaTimeSeriesExprMacro.class);
    public static final String FN_NAME = "gorilla_tsc_time_series_extract";
    @Override
    public String name() {
        return FN_NAME;
    }

    @Override
    public Expr apply(List<Expr> args) {

        if (args.size() != 6) {
            throw new IAE("Function[%s] must have 6 arguments", name());
        }

        final Expr columExpr = args.get(0);
        final Expr funExpr = args.get(1);
        final Expr startExpr = args.get(2);
        final Expr endExpr = args.get(3);
        final Expr intervalExpr = args.get(4);
        final Expr rangeExpr = args.get(5);

        String fun = (String) funExpr.getLiteralValue();
        long start = (long) startExpr.getLiteralValue();
        long end = (long) endExpr.getLiteralValue();
        long interval = (long) intervalExpr.getLiteralValue();
        long range = (long) rangeExpr.getLiteralValue();

        log.debug("fun:%s(%d,%d,%d,%d) is invoked",fun,start,end,interval,range);

        class  GorillaTimeSeriesExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr{

            private GorillaTimeSeriesExpr(Expr arg) {
                super(FN_NAME, arg);
            }

            @Override
            public ExprEval eval(ObjectBinding bindings) {

                Object val = arg.eval(bindings).value();
                if (val == null) {
                    // Return null if the argument if null.
                    return ExprEval.of(null);
                }
                TSG tsg = null;
                if(val instanceof TSG){
                    tsg = (TSG) val;
                }else if(val instanceof String){
                    String stringVal = (String) val;
                    final byte[] decoded = StringUtils.decodeBase64String(stringVal);
                    tsg = TSG.fromBytes(decoded);
                }else if(val instanceof byte[]){
                    tsg = TSG.fromBytes((byte[])val);
                }

                TreeMap<Long,Double> treeMap = new TreeMap<>();
                TSGIterator iterator = tsg.toIterator();
                while(iterator.hasNext()){
                    DataPoint dataPoint  =iterator.next();
                    if(dataPoint.getTime()>=start && dataPoint.getTime()<=end){
                        treeMap.put(dataPoint.getTime(),dataPoint.getValue());
                    }
                }

                if(treeMap.size() == 0){
                    return ExprEval.bestEffortOf(val);
                }

                tsg = new TSG(treeMap.firstKey()-treeMap.firstKey()%3600);
                for(Map.Entry<Long,Double> e:treeMap.entrySet()){
                    tsg.put(e.getKey(),e.getValue());
                }
                return ExprEval.of(StringUtils.encodeBase64String(tsg.toBytes()));
            }
            @Override
            public Expr visit(Shuttle shuttle) {
                Expr newArg = arg.visit(shuttle);
                return shuttle.visit(new GorillaTimeSeriesExpr(newArg));
            }
            @Nullable
            @Override
            public ExprType getOutputType(InputBindingInspector inspector)
            {
                return ExprType.fromValueType(ValueType.COMPLEX);
            }
        }
        return new GorillaTimeSeriesExpr(columExpr);
    }
}
