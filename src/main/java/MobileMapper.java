import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class MobileMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private FlowBean flowBean = new FlowBean();
    private Text keyText = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {


        String line = value.toString();
        String []fileds = StringUtils.split(line,'\t');

        //手机号
        String phoneNum = fileds[1];

        //上传流量
        long upFlow = Long.parseLong(fileds[fileds.length-3]);
        //下载流量
        long downFlow = Long.parseLong(fileds[fileds.length-2]);

       // flowBean.set(upFlow, downFlow);
        keyText.set(phoneNum);

        context.write(keyText,new FlowBean(upFlow,downFlow));
    }
}
