package org.apache.druid.query.aggregation.sql;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.GorillaTscAggregatorFactory;
import org.apache.druid.query.aggregation.GorillaTscPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import javax.annotation.Nullable;
import java.util.List;

public class GorillaTscSqlAggregator implements SqlAggregator {
    private static final String NAME = "GORILLA_TSC";
    private static final SqlAggFunction FUNCTION_INSTANCE = new GorillaTscSqlAggFunction();
    @Override
    public SqlAggFunction calciteFunction() {
        return FUNCTION_INSTANCE;
    }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(PlannerContext plannerContext, RowSignature rowSignature, VirtualColumnRegistry virtualColumnRegistry, RexBuilder rexBuilder, String name, AggregateCall aggregateCall, Project project, List<Aggregation> existingAggregations, boolean finalizeAggregations) {
        // This is expected to be a gorrila
        final DruidExpression input = Aggregations.toDruidExpressionForNumericAggregator(
                plannerContext,
                rowSignature,
                Expressions.fromFieldAccess(
                        rowSignature,
                        project,
                        aggregateCall.getArgList().get(0)
                )
        );
        if (input == null) {
            return null;
        }

        final AggregatorFactory aggregatorFactory;
        final String sketchName = StringUtils.format("%s:agg", name);




        final RexNode funArg = Expressions.fromFieldAccess(
                rowSignature,
                project,
                aggregateCall.getArgList().get(1)
        );

        if (!funArg.isA(SqlKind.LITERAL)) {
            return null;
        }

        final String fun = RexLiteral.stringValue(funArg);

        // this is expected to be quantile fraction
        final RexNode startArg = Expressions.fromFieldAccess(
                rowSignature,
                project,
                aggregateCall.getArgList().get(2)
        );

        if (!startArg.isA(SqlKind.LITERAL)) {
            // Quantile must be a literal in order to plan.
            return null;
        }

        final long start = ((Number) RexLiteral.value(startArg)).longValue();


        final RexNode endArg = Expressions.fromFieldAccess(
                rowSignature,
                project,
                aggregateCall.getArgList().get(3)
        );

        if (!endArg.isA(SqlKind.LITERAL)) {
            return null;
        }

        final long end =((Number) RexLiteral.value(endArg)).longValue();


        int interval = 0;
        if (aggregateCall.getArgList().size() > 4) {
            final RexNode intervalArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(4)
            );
            interval = ((Number) RexLiteral.value(intervalArg)).intValue();
        }

        int range = 0;
        if (aggregateCall.getArgList().size() > 5) {
            final RexNode rangeArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(5)
            );
            range = ((Number) RexLiteral.value(rangeArg)).intValue();
        }

        Integer maxIntermediateSize = GorillaTscAggregatorFactory.DEFAULT_MAX_INTERMEDIATE_SIZE;
        if (aggregateCall.getArgList().size() > 6) {
            final RexNode maxIntermediateSizeArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(6)
            );
            maxIntermediateSize = ((Number) RexLiteral.value(maxIntermediateSizeArg)).intValue();
            if(maxIntermediateSize == 0){
                maxIntermediateSize = GorillaTscAggregatorFactory.DEFAULT_MAX_INTERMEDIATE_SIZE;
            }
        }

        // Look for existing matching aggregatorFactory.
        for (final Aggregation existing : existingAggregations) {
            for (AggregatorFactory factory : existing.getAggregatorFactories()) {
                if (factory instanceof GorillaTscAggregatorFactory) {
                    final boolean matches = matchingAggregatorFactoryExists(
                            virtualColumnRegistry,
                            input,
                            maxIntermediateSize,
                            (GorillaTscAggregatorFactory) factory
                    );

                    if (matches) {
                        // Found existing one. Use this.
                        return Aggregation.create(
                                ImmutableList.of(),
                                new GorillaTscPostAggregator(
                                        name,
                                        new FieldAccessPostAggregator(
                                                factory.getName(),
                                                factory.getName()
                                        ),
                                        fun,start,end,interval,range
                                )
                        );
                    }
                }
            }
        }

        // No existing match found. Create a new one.
        if (input.isDirectColumnAccess()) {
            aggregatorFactory = new GorillaTscAggregatorFactory(
                    sketchName,
                    input.getDirectColumn(),
                    maxIntermediateSize
            );
        } else {
            VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
                    plannerContext,
                    input,
                    ValueType.COMPLEX
            );
            aggregatorFactory = new GorillaTscAggregatorFactory(
                    sketchName,
                    virtualColumn.getOutputName(),
                    maxIntermediateSize
            );
        }

        return Aggregation.create(
                ImmutableList.of(aggregatorFactory),
                new GorillaTscPostAggregator(
                        name,
                        new FieldAccessPostAggregator(
                                sketchName,
                                sketchName
                        ),
                        fun,start,end,interval,range
                )
        );
    }

    private boolean matchingAggregatorFactoryExists(VirtualColumnRegistry virtualColumnRegistry, DruidExpression input, Integer maxIntermediateSize, GorillaTscAggregatorFactory factory) {
        // Check input for equivalence.
        final boolean inputMatches;
        final VirtualColumn virtualInput =
                virtualColumnRegistry.findVirtualColumns(factory.requiredFields())
                        .stream()
                        .findFirst()
                        .orElse(null);

        if (virtualInput == null) {
            inputMatches = input.isDirectColumnAccess() && input.getDirectColumn().equals(factory.getFieldName());
        } else {
            inputMatches = ((ExpressionVirtualColumn) virtualInput).getExpression().equals(input.getExpression());
        }
        return inputMatches && maxIntermediateSize == factory.getMaxIntermediateSize();
    }

    private static class  GorillaTscSqlAggFunction extends SqlAggFunction{
        private static final String SIGNATURE1 = "'" + NAME + "(column)'\n";
        private static final String SIGNATURE2 = "'" + NAME + "(column,fun,start,end)'\n";
        private static final String SIGNATURE3 = "'" + NAME + "(column,fun,start,end,interval)'\n";
        private static final String SIGNATURE4 = "'" + NAME + "(column,fun,start,end, interval,range)'\n";
        private static final String SIGNATURE5 = "'" + NAME + "(column,fun,start,end, interval,range,maxIntermediateSize)'\n";

        GorillaTscSqlAggFunction()
        {
            super(
                    NAME,
                    null,
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.OTHER),
                    null,
                    OperandTypes.or(
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE1, OperandTypes.ANY),
                                    OperandTypes.family(SqlTypeFamily.ANY)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE2, OperandTypes.ANY, OperandTypes.LITERAL,OperandTypes.LITERAL, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING,SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE3, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE4, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL,OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE4, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL,OperandTypes.LITERAL,OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
                            )
                    ),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    false,
                    false
            );
        }
    }
}
