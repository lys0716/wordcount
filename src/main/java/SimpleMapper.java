/**
 * Created by yishuanglu on 5/9/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yishuanglu on 5/9/17.
 */
public class SimpleMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean toLowerCase;

    private Configuration conf;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        conf = context.getConfiguration();
        toLowerCase = conf.getBoolean("text.to.lowercase", true);
    }

    private String toLowerCase(String input) {
        return toLowerCase ? input.toLowerCase() : input;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //value = I love bittiger
        String line = toLowerCase(value.toString());
        String[] strArr = line.split(" ");
        for (String tmpStr : strArr) {
            word.set(tmpStr);
            context.write(word, one);
        }
    }
}

