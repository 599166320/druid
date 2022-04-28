package org.apache.druid.query.aggregation;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.query.core.DataPoint;
import org.apache.druid.query.core.InBitSet;
import org.apache.druid.query.core.TSG;
import org.apache.druid.query.core.TSGIterator;
import org.apache.druid.segment.ColumnValueSelector;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Iterator;

public class GorillaTscAggregator extends BaseGorillaTscAggregator<ColumnValueSelector>{

    public GorillaTscAggregator(ColumnValueSelector dimensionSelector) {
        super(dimensionSelector);
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        //buf和bufferAdd对应，保存的是中间结果
    }

    @Override
    void bufferAdd(ByteBuffer buf) {
        //buf和init对应，保存的是中间结果,目前中间件结果不使用buf来保存
        aggregate();
    }
    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position) {
        return tsg;
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
            //查询的时候做合并，就会执行一下代码
            TSG other = (TSG) obj;
            Iterator<DataPoint> tsgIterator =other.toIterator();
            while(tsgIterator.hasNext()){
                try {
                    tsg.put(tsgIterator.next());
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
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
        if(tsg != null && !tsg.isClosed()){
            tsg.close();
        }
    }




}
