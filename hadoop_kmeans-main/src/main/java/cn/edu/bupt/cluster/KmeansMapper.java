package cn.edu.bupt.cluster;


import cn.edu.bupt.util.CalUtil;
import cn.edu.bupt.util.DataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;

// map-reduce体系中map类
// 主要思路是读取中心点文件，将元素进行中心点归属判别，输出的key设置为中心点序号方便后续计算新中心点
public class KmeansMapper extends Mapper<Object, Text, Text, Text> {

    private ArrayList<ArrayList<Double>> centers = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读一下centers
        // 地址从配置中拿好了
        Configuration configuration = context.getConfiguration();
        String centerPath = configuration.get("cluster.center_path");
        centers = DataUtil.readCenter(centerPath);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList<Double> element = DataUtil.splitStringIntoArray(value.toString());
        // 选择最近中心点，将其作为key
        int index = CalUtil.selectNearestCenter(element, centers);
        context.write(new Text(Integer.toString(index)), value);
    }
}
