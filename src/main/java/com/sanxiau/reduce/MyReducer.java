package com.sanxiau.reduce;

import com.sanxiau.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text,FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        FlowBean flowBean = new FlowBean();
        long sumupflow = 0;
        long sumdownflow = 0;
        for(FlowBean val : values){
            sumupflow += val.getUpFlow();
            sumdownflow += val.getDownFlow();
        }
        flowBean.set(sumupflow,sumdownflow);
        context.write(key,flowBean);
    }
}
