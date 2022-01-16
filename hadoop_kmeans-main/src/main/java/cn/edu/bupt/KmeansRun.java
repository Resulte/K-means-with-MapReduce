package cn.edu.bupt;

import cn.edu.bupt.center.CenterRandomAdapter;
import cn.edu.bupt.cluster.KmeansAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

// kmeans主方法
// 主要就是循环调用直到次数或者停机状态
// 每次循环判断下停机
// data为空格连接
public class KmeansRun {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: KmeansRun <in> <out> <k>");
            System.exit(2);
        }

        // 命令行参数为数据集名称与聚类数
        String hdfsInput = otherArgs[0];
        String hdfsOutput = otherArgs[1];
        int k = Integer.parseInt(otherArgs[2]);
        System.out.println(k);

        String centerPath = hdfsOutput + "/centers.txt";
        String newCenterPath = hdfsOutput + "/new_centers.txt";
        String dataPath = hdfsInput;
        String clusterResultPath = hdfsOutput + "/kmeans_cluster_result.txt";

        // 初始化随机中心点
        CenterRandomAdapter.createRandomCenter(dataPath, centerPath, k);
        // 默认1000次，中途停退出
        for(int i=0;i<30;i++){
            System.out.println("round " + i);
            KmeansAdapter.start(dataPath, centerPath, newCenterPath);
            if(KmeansAdapter.checkStop(centerPath, newCenterPath))
                break;
        }
        KmeansAdapter.createClusterResult(dataPath, centerPath, clusterResultPath);
    }
}
