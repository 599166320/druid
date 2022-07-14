package org.apache.druid.promql.logical;

public class BinaryOperator implements Operator{
    private Operator left;
    private Operator rigth;
    private String op;

    public BinaryOperator(Operator left, Operator rigth, String op) {
        this.left = left;
        this.rigth = rigth;
        this.op = op;
    }

    @Override
    public Operator call() throws Exception {
        Operator operator = null;
        if("+".equals(op)){
            if(left instanceof NumberLiteralOperator && rigth instanceof NumberLiteralOperator){
                Number value = ((NumberLiteralOperator)left).getNumber().doubleValue()+((NumberLiteralOperator)rigth).getNumber().doubleValue();
                operator = new NumberLiteralOperator(value);
            }
        }else if("-".equals(op)){
            if(left instanceof NumberLiteralOperator && rigth instanceof NumberLiteralOperator){
                Number value = ((NumberLiteralOperator)left).getNumber().doubleValue()-((NumberLiteralOperator)rigth).getNumber().doubleValue();
                operator = new NumberLiteralOperator(value);
            }
        }else if("*".equals(op)){
            if(left instanceof NumberLiteralOperator && rigth instanceof NumberLiteralOperator){
                Number value = ((NumberLiteralOperator)left).getNumber().doubleValue()*((NumberLiteralOperator)rigth).getNumber().doubleValue();
                operator = new NumberLiteralOperator(value);
            }
        }else if("/".equals(op)){
            if(left instanceof NumberLiteralOperator && rigth instanceof NumberLiteralOperator){
                Number value = ((NumberLiteralOperator)left).getNumber().doubleValue()/((NumberLiteralOperator)rigth).getNumber().doubleValue();
                operator = new NumberLiteralOperator(value);
            }
        }else if(">".equals(op)){
            operator = new BooleanOperator(((NumberLiteralOperator)left).getNumber().doubleValue() > ((NumberLiteralOperator)rigth).getNumber().doubleValue());
        }else if(">=".equals(op)){
            operator = new BooleanOperator(((NumberLiteralOperator)left).getNumber().doubleValue() >= ((NumberLiteralOperator)rigth).getNumber().doubleValue());
        }else if("<".equals(op)){
            operator = new BooleanOperator(((NumberLiteralOperator)left).getNumber().doubleValue() < ((NumberLiteralOperator)rigth).getNumber().doubleValue());
        }else if("<=".equals(op)){
            operator = new BooleanOperator(((NumberLiteralOperator)left).getNumber().doubleValue() <= ((NumberLiteralOperator)rigth).getNumber().doubleValue());
        }else if("==".equals(op)){
            operator = new BooleanOperator(((NumberLiteralOperator)left).getNumber().doubleValue() == ((NumberLiteralOperator)rigth).getNumber().doubleValue());
        }else if("!=".equals(op)){
            operator = new BooleanOperator(((NumberLiteralOperator)left).getNumber().doubleValue() != ((NumberLiteralOperator)rigth).getNumber().doubleValue());
        }
        return operator;
    }
}
