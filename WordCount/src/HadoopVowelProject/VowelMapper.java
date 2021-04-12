package HadoopVowelProject;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.hsqldb.lib.StringUtil;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.io.IOException;
import java.util.Arrays;

public class VowelMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        context.write(new Text("c"), new IntWritable(countConsonants(line)));
        context.write(new Text("v"), new IntWritable(countVowels(line)));
    }
    private int countConsonants(String line){
        char[] chars = StringUtils.lowerCase(line).toCharArray();
        String regex = "[a-z&&[^aeiou]]";
        int counter = 0;
        for (char item: chars){
            if(Character.toString(item).matches(regex)){
                counter++;
            }
        }
        return counter;
    }
    private int countVowels(String line){
        char[] chars = StringUtils.lowerCase(line).toCharArray();
        List<Character> vowels = Arrays.asList(new Character[]{'a', 'e', 'i', 'o', 'u'});
        int counter = 0;
        for (char item: chars){
           if(vowels.contains(item)){
               counter++;
           }
        }
        return counter;
    }
}
