package org.apache.druid.collections;
import com.google.common.collect.ImmutableList;
import org.apache.druid.query.scan.ScanQuery;
import org.junit.Assert;
import org.junit.Test;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MultiColumnSorterTests {

    @Test
    public void test(){
        List<String> orderByDirection = ImmutableList.of("ASCENDING","DESCENDING","DESCENDING");
        Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>>() {
            @Override
            public int compare(MultiColumnSorter.MultiColumnSorterElement<Integer> o1, MultiColumnSorter.MultiColumnSorterElement<Integer> o2) {
                for(int i = 0; i < o1.getOrderByColumValues().size() ; i++){
                    if(!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))){
                        if(ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))){
                            return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
                        }else{
                            return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
                        }
                    }
                }
                return 0;
            }
        };
        MultiColumnSorter multiColumnSorter = new MultiColumnSorter(5, comparator);
        multiColumnSorter.add(1,ImmutableList.of(0,0,1));
        multiColumnSorter.add(2,ImmutableList.of(0,0,2));
        multiColumnSorter.add(3,ImmutableList.of(0,0,3));
        multiColumnSorter.add(4,ImmutableList.of(0,0,4));
        multiColumnSorter.add(5,ImmutableList.of(0,3,5));
        multiColumnSorter.add(6,ImmutableList.of(0,6,7));
        multiColumnSorter.add(7,ImmutableList.of(0,0,8));
        multiColumnSorter.add(1,ImmutableList.of(0,0,9));
        multiColumnSorter.add(100,ImmutableList.of(1,0,0));
        multiColumnSorter.add(1,ImmutableList.of(0,0,1));
        multiColumnSorter.add(1,ImmutableList.of(0,0,3));
        multiColumnSorter.add(9,ImmutableList.of(0,0,6));
        multiColumnSorter.add(11,ImmutableList.of(0,0,6));
        Iterator<Integer> it = multiColumnSorter.drain();
        List<Integer> expectedValues = ImmutableList.of(6,5,1,7,11);
        int i = 0;
        while(it.hasNext()){
            Assert.assertEquals(expectedValues.get(i++), it.next());
        }
    }
}
