package HadoopVowelProject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class VowelReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalCnt = 0;
        // Iterator is an object that can be used to loop through collections
        Iterator<IntWritable> iterator = values.iterator();
        // .hasNext returns true if the iteration has more elements.
        while(iterator.hasNext()){
            // .next returns the next element in the iteration.
            IntWritable next = iterator.next();
            // IntWritable.get() returns the value of the IntWritable.
            totalCnt += next.get();
        }
        context.write(key, new IntWritable(totalCnt));
    }
}
