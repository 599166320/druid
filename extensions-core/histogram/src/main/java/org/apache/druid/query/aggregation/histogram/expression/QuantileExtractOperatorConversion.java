package org.apache.druid.query.aggregation.histogram.expression;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

public class QuantileExtractOperatorConversion extends DirectOperatorConversion {

    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder(StringUtils.toUpperCase(QuantileExtractExprMacro.FN_NAME))
            .operandTypes(
                    SqlTypeFamily.ANY,
                    SqlTypeFamily.CHARACTER
            )
            .returnTypeNullable(SqlTypeName.OTHER)
            .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .build();

    public QuantileExtractOperatorConversion() {
        super(SQL_FUNCTION, QuantileExtractExprMacro.FN_NAME);
    }

    @Override
    public SqlOperator calciteOperator() {
        return SQL_FUNCTION;
    }

}
