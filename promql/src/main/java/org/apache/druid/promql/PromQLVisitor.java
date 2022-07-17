package org.apache.druid.promql;
import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.antlr.PromQLParserBaseVisitor;
import org.apache.druid.promql.data.Series;
import org.apache.druid.promql.logical.*;
import org.apache.druid.promql.util.Quantile;
import org.apache.druid.promql.util.TSGWindowUtil;

import java.io.*;
import java.util.*;
public class PromQLVisitor extends PromQLParserBaseVisitor<Operator> {

    private int step = 15;
    private Map<String,SeriesSetOperator> rawSeriesSetOperatorMap = new HashMap<>();

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public Map<String, SeriesSetOperator> getRawSeriesSetOperatorMap() {
        return rawSeriesSetOperatorMap;
    }

    public void setRawSeriesSetOperatorMap(Map<String, SeriesSetOperator> rawSeriesSetOperatorMap) {
        this.rawSeriesSetOperatorMap = rawSeriesSetOperatorMap;
    }

    @Override
    public Operator visitExpression(PromQLParser.ExpressionContext ctx)
    {
        if(ctx.vectorOperation() != null){
            try {
                return visitVectorOperation(ctx.vectorOperation()).call();
            }catch (Exception e){
                throw new RuntimeException(e.getMessage());
            }
        }
        Operator operator = super.visitExpression(ctx);
        return operator;
    }

    @Override
    public Operator visitVectorOperation(PromQLParser.VectorOperationContext ctx)
    {
        if(ctx.vectorOperation().size() == 2){
            Operator left = visitVectorOperation(ctx.vectorOperation().get(0));
            Operator rigth = visitVectorOperation(ctx.vectorOperation().get(1));
            if(ctx.addOp() != null){
                try {
                    return new BinaryOperator(left,rigth,ctx.addOp().getText()).call();
                }catch (Exception e){
                    throw new RuntimeException(e.getMessage());
                }
            }else if(ctx.multOp() != null){
                try {
                    return new BinaryOperator(left,rigth,ctx.multOp().getText()).call();
                }catch (Exception e){
                    throw new RuntimeException(e.getMessage());
                }
            }else if(ctx.compareOp() != null){
                try {
                    return new BinaryOperator(left,rigth,ctx.compareOp().getText()).call();
                }catch (Exception e){
                    throw new RuntimeException(e.getMessage());
                }
            }
        }
        Operator operator = super.visitVectorOperation(ctx);
        return operator;
    }

    /**
     * function,agg -->SeriesSetOperator
     * @param ctx
     * @return
     */
    @Override
    public Operator visitVector(PromQLParser.VectorContext ctx) {
        Operator operator = null;
        if(ctx.instantSelector() != null){
            operator = (InstantOperator) visitInstantSelector(ctx.instantSelector());
        }else if(ctx.matrixSelector() != null){
            operator = (InstantOperator) visitMatrixSelector(ctx.matrixSelector());
        }else if(ctx.function_() != null){
            operator = visitFunction_(ctx.function_());
        }else if(ctx.aggregation() != null){
            operator = visitAggregation(ctx.aggregation());
        }else if(ctx.literal() != null){
            operator = visitLiteral(ctx.literal());
        }

        try {
            if(operator != null){
                return operator;
            }
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
        return super.visitVector(ctx);
    }

    @Override
    public Operator visitAggregation(PromQLParser.AggregationContext ctx) {
        try {
            String agg = ctx.AGGREGATION_OPERATOR().getText();
            List<PromQLParser.ParameterContext> parameterContextList = ctx.parameterList().parameter();
            for(PromQLParser.ParameterContext parameterContext:parameterContextList){

                Operator vectorOperation = visitVectorOperation(parameterContext.vectorOperation());
                if(vectorOperation instanceof MatrixOperator){
                    throw new RuntimeException("parse error: expected type instant vector in aggregation expression, got range vector");
                }
                SeriesSetOperator operator = (SeriesSetOperator) vectorOperation.call();
                List<Series> seriesSet = operator.getSeriesSet();
                AggregationOperator aggregationOperator = AggregationOperatorFactory.createAggregationOperatorByName(agg);
                aggregationOperator.setSeriesSet(seriesSet);
                if(ctx.by() != null){
                    List<String> groubBys = new ArrayList<>();
                    List<PromQLParser.LabelNameContext>  labelNameContextList = ctx.by().labelNameList().labelName();
                    for(PromQLParser.LabelNameContext labelNameContext:labelNameContextList){
                        groubBys.add(labelNameContext.METRIC_NAME().getText());
                    }
                    aggregationOperator.setGroubBys(groubBys);
                }
                try {
                    return aggregationOperator.call();
                }catch (Exception e){
                    throw new RuntimeException(e.getMessage());
                }
            }
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
        return super.visitAggregation(ctx);
    }

    @Override
    public Operator visitMatrixSelector(PromQLParser.MatrixSelectorContext ctx) {
        int range;
        MatrixOperator matrixOperator = new MatrixOperator();
        visitInstantSelector(ctx.instantSelector(),matrixOperator);
        String rangeText = ctx.TIME_RANGE().getText().replaceAll("\\[","").replaceAll("]","");
        if(rangeText.endsWith("s")){
            range = Integer.parseInt(rangeText.replaceAll("s",""));
        }else {
            range = Integer.parseInt(rangeText.replaceAll("m","")) * 60;
        }
        matrixOperator.setRange(range);
        matrixOperator.getSeriesSetOperator().setResultType("matrix");
        return matrixOperator;
    }

    @Override
    public Operator visitInstantSelector(PromQLParser.InstantSelectorContext ctx) {
        InstantOperator instantOperator = new InstantOperator();
        visitInstantSelector(ctx,instantOperator);
        return instantOperator;
    }

    public void visitInstantSelector(PromQLParser.InstantSelectorContext ctx, InstantOperator instantOperator){
        TreeSet<String> keySet = new TreeSet<>();
        String metric = null;
        if(ctx.METRIC_NAME() != null){
            metric = ctx.METRIC_NAME().getSymbol().getText();
            keySet.add("name="+metric);
        }

        List<Map<String,Object>> whereMap = new ArrayList<>();
        if(ctx.labelMatcherList() != null){
            List<PromQLParser.LabelMatcherContext> labelMatcherContexts =  ctx.labelMatcherList().labelMatcher();
            for(PromQLParser.LabelMatcherContext labelMatcherContext:labelMatcherContexts){
                Map<String,Object> labelValue = new HashMap<>();
                String labelName = labelMatcherContext.labelName().getText();
                if("__name__".equals(labelName)){
                    metric = labelMatcherContext.STRING().getText().replaceAll("\"","");
                    labelName = "name";
                }
                labelValue.put("label",labelName);
                labelValue.put("operator",labelMatcherContext.labelMatcherOperator().getText());
                labelValue.put("value",labelMatcherContext.STRING().getText());
                whereMap.add(labelValue);
                keySet.add(labelName+","+labelMatcherContext.labelMatcherOperator().getText()+","+labelMatcherContext.STRING().getText());
            }
        }
        if(Objects.isNull(metric)){
            throw new RuntimeException("The metric name cannot be empty.");
        }
        instantOperator.setMetric(metric);
        instantOperator.setWhere(whereMap);
        //instantOperator.setKey(String.join(",",keySet));
        ObjectInputStream objectInputStream = null;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        ){
            objectOutputStream.writeObject(rawSeriesSetOperatorMap.get(String.join(",",keySet)));
            ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            objectInputStream = new ObjectInputStream(inputStream);
            SeriesSetOperator seriesSetOperator = (SeriesSetOperator)objectInputStream.readObject();
            seriesSetOperator.setResultType("vector");
            instantOperator.setSeriesSetOperator(seriesSetOperator);
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }finally {
            if(objectInputStream != null){
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }
    }

    @Override
    public Operator visitFunction_(PromQLParser.Function_Context ctx) {
        List<PromQLParser.ParameterContext> parameterContextList = ctx.parameter();
        String fun = ctx.FUNCTION().getText();
        if("histogram_quantile".equals(fun)){
            Number quantile = 0;
            SeriesSetOperator seriesSetOperator = null;
            for(PromQLParser.ParameterContext param:parameterContextList){
                if(param.literal() != null){
                    quantile = Double.valueOf(param.literal().NUMBER().getText());
                }else if(param.vectorOperation() != null){
                    seriesSetOperator = (SeriesSetOperator) visitVectorOperation(param.vectorOperation());
                }
            }
            //计算分位置
            List<Series> seriesList = seriesSetOperator.getSeriesSet();
            List<Series> qseriesList = new ArrayList<>();
            //相同label,不同le放在一个列表List<Quantile.Bucket>中
            Map<String,List<Series>> seriesGroup = new HashMap<>();
            for(Series series:seriesList){
                TreeMap<String, Object> treeMap = series.getLabels();
                StringBuilder stringBuilder = new StringBuilder();
                for(Map.Entry<String,Object> lableValue:treeMap.entrySet()){
                    if("le".equals(lableValue.getKey()) && treeMap.size() == 1){
                        stringBuilder.append("");
                    }else if(!"le".equals(lableValue.getKey())){
                        if(stringBuilder.length() > 0){
                            stringBuilder.append(",").append(lableValue.getKey()).append(":").append(lableValue.getValue());
                        }else {
                            stringBuilder.append(lableValue.getKey()).append(":").append(lableValue.getValue());
                        }
                    }else {
                        continue;
                    }
                    List<Series> seseriesGroup;
                    if(seriesGroup.containsKey(stringBuilder.toString())){
                        seseriesGroup = seriesGroup.get(stringBuilder.toString());
                    }else {
                        seseriesGroup = new ArrayList<>();
                        seriesGroup.put(stringBuilder.toString(),seseriesGroup);
                    }
                    seseriesGroup.add(series);
                }
            }
            //seriesGroup--->key:List<Series>
            //seriesList---->
            Collection<List<Series>> seriesCollections = seriesGroup.values();
            for(List<Series> seriesCollection:seriesCollections){
                //按照时间分组：ts---List<Quantile.Bucket>
                Map<Integer,List<Quantile.Bucket>> tsBuckets = new HashMap<>();
                //同一组,取出相同时间戳的value和le
                //对每一个列表计算分位值
                TreeMap<String,Object> qlabels = new TreeMap<>();
                for(Series ssl:seriesCollection){
                    TreeMap<Integer, Double> dp = ssl.getDataPoint();
                    TreeMap<String, Object> lb = ssl.getLabels();
                    Double le;
                    if("+Inf".equals(lb.get("le"))){
                        le = Double.POSITIVE_INFINITY;
                    }else {
                        le = Double.valueOf(lb.get("le").toString());
                    }
                    qlabels = lb;
                    for(Map.Entry<Integer, Double> de:dp.entrySet()){
                        if(tsBuckets.containsKey(de.getKey())){
                            List<Quantile.Bucket> bucketList = tsBuckets.get(de.getKey());
                            bucketList.add(new Quantile.Bucket(le,de.getValue()));
                            tsBuckets.put(de.getKey(),bucketList);
                        }else {
                            List<Quantile.Bucket> bucketList = new ArrayList<>();
                            tsBuckets.put(de.getKey(),bucketList);
                            bucketList.add(new Quantile.Bucket(le,de.getValue()));
                        }
                    }
                }
                Series qSeries = new Series();
                qseriesList.add(qSeries);
                TreeMap<Integer,Double> qdataPoint = new TreeMap<>();
                qlabels.remove("le");
                qSeries.setLabels(qlabels);
                qSeries.setDataPoint(qdataPoint);
                for(Map.Entry<Integer,List<Quantile.Bucket>> bucketEntry:tsBuckets.entrySet()){
                    double q = Quantile.bucketQuantile(quantile.doubleValue(),bucketEntry.getValue());
                    qdataPoint.put(bucketEntry.getKey(),q);
                }
            }
            seriesSetOperator.setSeriesSet(qseriesList);
            return seriesSetOperator;
        }

        Operator vectorOperator = visitVectorOperation(parameterContextList.get(0).vectorOperation());
        if(!(vectorOperator instanceof MatrixOperator)){
            throw new RuntimeException("parse error: expected type range vector in call to function "+fun+", got instant vector");
        }
        MatrixOperator operator = (MatrixOperator)vectorOperator;
        SeriesSetOperator seriesSetOperator;
        try {
            seriesSetOperator = (SeriesSetOperator) operator.call();
            List<Series> seriesSet = seriesSetOperator.getSeriesSet();
            for(Series series:seriesSet){
                TreeMap<Integer, Double> datapoints = series.getDataPoint();
                datapoints = TSGWindowUtil.slidingWindow(datapoints,operator.getRange(),step,TSGWindowUtil.getFun(fun));
                series.setDataPoint(datapoints);
            }
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
        return seriesSetOperator;
    }

    @Override
    public Operator visitLiteral(PromQLParser.LiteralContext ctx) {
        return new NumberLiteralOperator(Double.valueOf(ctx.NUMBER().getText()));
    }
}
