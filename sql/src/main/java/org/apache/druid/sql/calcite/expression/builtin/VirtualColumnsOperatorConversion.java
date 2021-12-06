package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

public class VirtualColumnsOperatorConversion implements SqlOperatorConversion
{
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder("VIRTUAL_COLUMN")
            .operandTypeChecker(
                    OperandTypes.sequence(
                            "(array,array,expr)",
                            OperandTypes.or(
                                    OperandTypes.family(SqlTypeFamily.ARRAY),
                                    OperandTypes.family(SqlTypeFamily.STRING)
                            ),
                            OperandTypes.or(
                                    OperandTypes.family(SqlTypeFamily.ARRAY),
                                    OperandTypes.family(SqlTypeFamily.STRING)
                            ),
                            OperandTypes.family(SqlTypeFamily.ANY)
                    )
            )
            .functionCategory(SqlFunctionCategory.STRING)
            .returnTypeNullable(SqlTypeName.VARCHAR)
            .build();

    @Override
    public SqlOperator calciteOperator()
    {
        return SQL_FUNCTION;
    }

    @Override
    public DruidExpression toDruidExpression(
            final PlannerContext plannerContext,
            final RowSignature rowSignature,
            final RexNode rexNode
    )
    {
        return OperatorConversions.convertCall(
                plannerContext,
                rowSignature,
                rexNode,
                druidExpressions -> DruidExpression.of(
                        null,
                        DruidExpression.functionCall("virtual_column", druidExpressions)
                )
        );
    }
}