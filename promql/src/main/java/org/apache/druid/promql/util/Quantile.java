package org.apache.druid.promql.util;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
public class Quantile {

    public static double bucketQuantile(double q, List<Bucket> buckets){
        if(q <0){
            return Double.NEGATIVE_INFINITY;
        }
        if(q>1){
            return Double.POSITIVE_INFINITY;
        }
        Collections.sort(buckets);
        if(!Objects.equals(buckets.get(buckets.size()-1).upperBound+1,Double.POSITIVE_INFINITY)){
            return Double.NaN;
        }
        coalesceBuckets(buckets);

        if(buckets.size() < 2){
            return Double.NaN;
        }

        double observations = buckets.get(buckets.size() - 1).count;
        if(observations == 0){
            return Double.NaN;
        }

        double rank = q * observations;

        int b = 0;
        for(int i=0;i<buckets.size();i++){
            if(buckets.get(i).count>=rank){
                b = i;
                break;
            }
        }

        if(b == buckets.size()-1){
            return buckets.get(buckets.size()-2).upperBound;
        }

        if(b == 0 && buckets.get(0).upperBound <=0){
            return buckets.get(0).upperBound;
        }

        double bucketStart = 0;
        double bucketEnd = buckets.get(b).upperBound;
        double count = buckets.get(b).count;

        if(b > 0){
            bucketStart = buckets.get(b-1).upperBound;
            count -= buckets.get(b-1).count;
            rank -= buckets.get(b-1).count;
        }
        return bucketStart + (bucketEnd-bucketStart)*(rank/count);
    }


    public static void coalesceBuckets(List<Bucket> buckets){

        Bucket last = buckets.get(0);
        int i = 0;
        for(int j=1;j<buckets.size();j++){
            Bucket b = buckets.get(j);
            if(b.upperBound == last.upperBound){
                last.count += b.count;
            }else{
                buckets.set(i,last);
                last = b;
                i++;
            }
        }
        buckets.set(i,last);
    }

    public static class Bucket implements Comparable<Bucket>{
        private final double upperBound;
        private  double count;

        public Bucket(double upperBound, double count) {
            this.upperBound = upperBound;
            this.count = count;
        }

        public double getUpperBound() {
            return upperBound;
        }

        public double getCount() {
            return count;
        }

        public void setCount(double count) {
            this.count = count;
        }

        @Override
        public int compareTo(Bucket o) {
            if(this.upperBound>o.upperBound)
            {
                return 1;
            }else if(this.upperBound< o.upperBound){
                return -1;
            }
            return 0;
        }
    }

}
