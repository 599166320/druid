/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.math.expr;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

class TernaryExpr implements Expr
{
  protected final Expr logicalExpr;
  protected final Expr firstExpr;
  protected final Expr secondExpr;

  TernaryExpr(Expr logicalExpr, Expr firstExpr, Expr secondExpr){
    this.logicalExpr = logicalExpr;
    this.firstExpr = firstExpr;
    this.secondExpr = secondExpr;
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval logicalVal = logicalExpr.eval(bindings);
    return logicalVal.asBoolean() ? firstExpr.eval(bindings) : secondExpr.eval(bindings);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s %s %s)", logicalExpr, firstExpr, secondExpr);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("(%s %s %s)", logicalExpr.stringify(), firstExpr.stringify(), secondExpr.stringify());
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    Expr newLogicalExpr = logicalExpr.visit(shuttle);
    Expr newFirstExpr = firstExpr.visit(shuttle);
    Expr newsecondExpr = secondExpr.visit(shuttle);
    //noinspection ObjectEquality (checking for object equality here is intentional)
    if (logicalExpr != newLogicalExpr || firstExpr != newFirstExpr || secondExpr != newsecondExpr) {
      return shuttle.visit(new TernaryExpr(newLogicalExpr, newFirstExpr, newsecondExpr));
    }
    return shuttle.visit(this);
  }

  @Override
  public BindingAnalysis analyzeInputs()
  {
    return logicalExpr.analyzeInputs().with(firstExpr).with(secondExpr).withScalarArguments(ImmutableSet.of(logicalExpr, firstExpr, secondExpr));
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    ExprType firstExprType = firstExpr.getOutputType(inspector);
    ExprType secondExprType = secondExpr.getOutputType(inspector);
    if (firstExprType.id != secondExprType.id){
      throw new IAE("The return values of the first and second expressions must be the same.");
    }
    return firstExprType;
  }
}
