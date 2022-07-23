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
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.quantile.ValueAppendAggregatorFactory;
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

public class QuantileExactSqlAggregator implements SqlAggregator{
    private static final String NAME = "SUMMARY_EXACT";
    private static final SqlAggFunction FUNCTION_INSTANCE = new QuantileExactSqlAggFunction();
    @Override
    public SqlAggFunction calciteFunction() {
        return FUNCTION_INSTANCE;
    }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(PlannerContext plannerContext, RowSignature rowSignature, VirtualColumnRegistry virtualColumnRegistry, RexBuilder rexBuilder, String name, AggregateCall aggregateCall, Project project, List<Aggregation> existingAggregations, boolean finalizeAggregations) {
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


        Integer maxIntermediateSize = ValueAppendAggregatorFactory.DEFAULT_MAX_INTERMEDIATE_SIZE;
        if (aggregateCall.getArgList().size() > 1) {
            final RexNode maxIntermediateSizeArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(1)
            );
            maxIntermediateSize = ((Number) RexLiteral.value(maxIntermediateSizeArg)).intValue();
            if(maxIntermediateSize == 0){
                maxIntermediateSize = ValueAppendAggregatorFactory.DEFAULT_MAX_INTERMEDIATE_SIZE;
            }
        }

        // Look for existing matching aggregatorFactory.
        for (final Aggregation existing : existingAggregations) {
            for (AggregatorFactory factory : existing.getAggregatorFactories()) {
                if (factory instanceof ValueAppendAggregatorFactory) {
                    final boolean matches = matchingAggregatorFactoryExists(
                            virtualColumnRegistry,
                            input,
                            maxIntermediateSize,
                            (ValueAppendAggregatorFactory) factory
                    );

                    if (matches) {
                        // Found existing one. Use this.
                        return Aggregation.create(
                                ImmutableList.of(),
                                new QuantileExactPostAggregator(
                                        name,
                                        new FieldAccessPostAggregator(
                                                factory.getName(),
                                                factory.getName()
                                        ),
                                        fun
                                )
                        );
                    }
                }
            }
        }

        // No existing match found. Create a new one.
        if (input.isDirectColumnAccess()) {
            aggregatorFactory = new ValueAppendAggregatorFactory(
                    sketchName,
                    input.getDirectColumn(),
                    maxIntermediateSize,
                    fun
            );
        } else {
            VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
                    plannerContext,
                    input,
                    ValueType.COMPLEX
            );
            aggregatorFactory = new ValueAppendAggregatorFactory(
                    sketchName,
                    virtualColumn.getOutputName(),
                    maxIntermediateSize,
                    fun
            );
        }

        return Aggregation.create(
                ImmutableList.of(aggregatorFactory),
                new QuantileExactPostAggregator(
                        name,
                        new FieldAccessPostAggregator(
                                sketchName,
                                sketchName
                        ),
                        fun
                )
        );
    }

    private boolean matchingAggregatorFactoryExists(VirtualColumnRegistry virtualColumnRegistry, DruidExpression input, Integer maxIntermediateSize, ValueAppendAggregatorFactory factory) {
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

    private static class  QuantileExactSqlAggFunction extends SqlAggFunction{
        private static final String SIGNATURE1 = "'" + NAME + "(column)'\n";
        QuantileExactSqlAggFunction()
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
                            )
                    ),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    false,
                    false
            );
        }
    }
}
