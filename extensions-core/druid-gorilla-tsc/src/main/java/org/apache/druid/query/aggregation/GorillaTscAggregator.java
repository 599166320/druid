package org.apache.druid.query.aggregation;
import org.apache.druid.query.core.DataPoint;
import org.apache.druid.query.core.TSG;
import org.apache.druid.segment.ColumnValueSelector;
import javax.annotation.Nullable;
public class GorillaTscAggregator extends BaseGorillaTscAggregator<ColumnValueSelector>{

    public GorillaTscAggregator(ColumnValueSelector dimensionSelector,int maxNumEntries, boolean onHeap) {
        super(dimensionSelector,maxNumEntries,onHeap);
    }
    /**
     * 新数据加入的时候会调用
     */
    @Override
    public void aggregate() {
        Object obj = selector.getObject();
        if(tsg == null && obj != null){
            if(obj instanceof Object[][]){
                Object[][] timeAndValues = (Object[][]) obj;
                if(timeAndValues.length > 0 && timeAndValues[0].length> 0){
                    long ts = (long) (timeAndValues)[0][0];
                    tsg = new TSG(ts-ts%(3600));
                }else{
                    return;
                }
            }else if(obj instanceof  TSG){
                tsg = (TSG) obj;
                return;
            }
        }

        if (obj == null) {
            return;
        }else if(obj instanceof Object[][]){
            Object[][] timeAndValues = (Object[][]) obj;
            for (Object[] timeAndValue : timeAndValues) {
                tsg.put((Long) timeAndValue[0], (Double) timeAndValue[1]);
            }
        }else if(obj instanceof TSG){
            //查询的时候做合并，就会执行一下代码,都是大块的合并
            TSG other = (TSG) obj;
            tsg = TSG.merge(tsg,other);//sum,avg,等其他函数
        }else if(obj instanceof DataPoint){
            tsg.put((DataPoint)obj);
        }
    }

    @Nullable
    @Override
    public Object get() {
        return tsg;
    }

    @Override
    public void close() {
        /*
        if(tsg != null && !tsg.isClosed()){
            tsg.close();
        }*/
        tsg = null;
    }
}
