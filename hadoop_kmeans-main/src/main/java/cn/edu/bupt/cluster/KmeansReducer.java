package cn.edu.bupt.cluster;

import cn.edu.bupt.util.CalUtil;
import cn.edu.bupt.util.DataUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

// reduce，主要思路是把同一key，也就是同index的元素相加算新的中心点
public class KmeansReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<Double> sumElement = new ArrayList<Double>();
        int num = 0;
        // 遍历values相加，求新中心点
        for(Text t:values){
            num += 1;
            ArrayList<Double> element = DataUtil.splitStringIntoArray(t.toString());
            if(sumElement.size() <= 0){
                sumElement = new ArrayList<Double>(element);
                continue;
            }
            CalUtil.addElement(sumElement, element);
        }
        CalUtil.calCenter(num, sumElement);
        // 存放新中心点
        context.write(new Text(""), new Text(DataUtil.convertArrayIntoString(sumElement)));
    }
}
