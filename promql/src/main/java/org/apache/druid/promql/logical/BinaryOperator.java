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
            }else if(left instanceof NumberLiteralOperator && rigth instanceof SeriesSetOperator){
                operator = ((SeriesSetOperator)rigth).add((NumberLiteralOperator) left);
            }else if(left instanceof SeriesSetOperator && rigth instanceof NumberLiteralOperator){
                operator = ((SeriesSetOperator)left).add((NumberLiteralOperator) rigth);
            }else {
                operator = ((SeriesSetOperator)left).add((SeriesSetOperator)rigth);
            }
        }else if("-".equals(op)){
            if(left instanceof NumberLiteralOperator && rigth instanceof NumberLiteralOperator){
                Number value = ((NumberLiteralOperator)left).getNumber().doubleValue()-((NumberLiteralOperator)rigth).getNumber().doubleValue();
                operator = new NumberLiteralOperator(value);
            }else if(left instanceof NumberLiteralOperator && rigth instanceof SeriesSetOperator){
                operator = SeriesSetOperator.sub((NumberLiteralOperator) left,((SeriesSetOperator)rigth));
            }else if(left instanceof SeriesSetOperator && rigth instanceof NumberLiteralOperator){
                operator = SeriesSetOperator.sub(((SeriesSetOperator)left),((NumberLiteralOperator) rigth));
            }else {
                operator = ((SeriesSetOperator)left).sub((SeriesSetOperator)rigth);
            }
        }else if("*".equals(op)){
            if(left instanceof NumberLiteralOperator && rigth instanceof NumberLiteralOperator){
                Number value = ((NumberLiteralOperator)left).getNumber().doubleValue()*((NumberLiteralOperator)rigth).getNumber().doubleValue();
                operator = new NumberLiteralOperator(value);
            }else if(left instanceof NumberLiteralOperator && rigth instanceof SeriesSetOperator){
                operator = ((SeriesSetOperator)rigth).mul((NumberLiteralOperator) left);
            }else if(left instanceof SeriesSetOperator && rigth instanceof NumberLiteralOperator){
                operator = ((SeriesSetOperator)left).mul((NumberLiteralOperator) rigth);
            }else {
                operator = ((SeriesSetOperator)left).mul((SeriesSetOperator)rigth);
            }
        }else if("/".equals(op)){
            if(left instanceof NumberLiteralOperator && rigth instanceof NumberLiteralOperator){
                Number value = ((NumberLiteralOperator)left).getNumber().doubleValue()/((NumberLiteralOperator)rigth).getNumber().doubleValue();
                operator = new NumberLiteralOperator(value);
            }else if(left instanceof NumberLiteralOperator && rigth instanceof SeriesSetOperator){
                operator = SeriesSetOperator.div((NumberLiteralOperator) left,((SeriesSetOperator)rigth));
            }else if(left instanceof SeriesSetOperator && rigth instanceof NumberLiteralOperator){
                operator = SeriesSetOperator.div(((SeriesSetOperator)left),((NumberLiteralOperator) rigth));
            }else {
                operator = ((SeriesSetOperator)left).div((SeriesSetOperator)rigth);
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
