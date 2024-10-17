package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

public class InOptionCounter extends SqlBasicVisitor<List<Integer>>
{
  private final List<Integer> inCounter = new ArrayList<>();

  @Override
  public List<Integer> visit(SqlCall call)
  {
    if (call.getKind() == SqlKind.IN) {
      SqlBasicCall inCall = (SqlBasicCall) call;
      if (inCall.getOperandList().size() > 1) {
        List<SqlNode> options = inCall.getOperandList().subList(1, inCall.getOperandList().size());
        for (SqlNode node : options) {
          if (node instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) node;
            inCounter.add(sqlNodeList.size());
          }
        }
      }
    }

    for (SqlNode operand : call.getOperandList()) {
      if (operand != null) {
        operand.accept(this);
      }
    }
    return inCounter;
  }
}
