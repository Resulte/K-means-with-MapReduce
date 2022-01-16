package cn.edu.bupt.cluster;

import cn.edu.bupt.KmeansRun;
import cn.edu.bupt.util.CalUtil;
import cn.edu.bupt.util.DataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

// 主要是把map和reduce的流程组装起来，顺便写个main
public class KmeansAdapter {

    // 计算是否能停机
    // 检查中心点相同情况，停止
    // 如果不相同，利用新center去覆盖旧center（未完成）
    public static boolean checkStop(String centerPath, String newCenterPath){
        // 读取新旧center文件
        try {
            ArrayList<ArrayList<Double>> newCenters = DataUtil.readCenter(newCenterPath);
            ArrayList<ArrayList<Double>> centers = DataUtil.readCenter(centerPath);
            // 获取距离信息
            double distanceSum = CalUtil.calDistanceBetweenCenters(centers, newCenters);
            if(distanceSum == 0){
                // 停机，不做修改
                return true;
            }else{
                // 覆盖原中心文件
                System.out.println("distanceSum=" + distanceSum);
                DataUtil.changeCenters(centerPath, newCenterPath, new Configuration());
                return false;
            }
        } catch (IOException e) {
            System.out.println(centerPath + " ! file wrong");
            e.printStackTrace();
        }
        return true;
    }

    // 一次迭代流程
    // map读取中心，分类，reduce计算新中心，存储
    // 比较两次中心差距，存储新中心点
    public static void start(String dataPath, String centerPath, String newCenterPath){
        // 设置原中心点
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("cluster.center_path", centerPath);

        try {
            Job job = Job.getInstance(hadoopConfig, "one round cluster task");

            job.setJarByClass(KmeansRun.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 输出为新计算得到的center，已存在则删除
            Path outPath = new Path(newCenterPath);
            outPath.getFileSystem(hadoopConfig).delete(outPath, true);

            //job执行作业时输入和输出文件的路径
            FileInputFormat.addInputPath(job, new Path(dataPath));
            FileOutputFormat.setOutputPath(job, new Path(newCenterPath));

            //执行job，直到完成
            job.waitForCompletion(true);
            System.out.println("finish one round cluster task");
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 根据center文件，生成聚类结果
    // 利用mapper即可
    public static void createClusterResult(String dataPath, String centerPath, String clusterResultPath){
        // 设置原中心点
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("cluster.center_path", centerPath);

        try {
            Job job = Job.getInstance(hadoopConfig, "cluster result task");

            job.setJarByClass(KmeansRun.class);
            // 无reducer
            job.setMapperClass(KmeansMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 输出为新计算得到的center，已存在则删除
            Path outPath = new Path(clusterResultPath);
            outPath.getFileSystem(hadoopConfig).delete(outPath, true);

            //job执行作业时输入和输出文件的路径
            FileInputFormat.addInputPath(job, new Path(dataPath));
            FileOutputFormat.setOutputPath(job, new Path(clusterResultPath));

            //执行job，直到完成
            job.waitForCompletion(true);
            System.out.println("cluster result task finished");

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
