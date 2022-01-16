package cn.edu.bupt.center;

import cn.edu.bupt.KmeansRun;
import cn.edu.bupt.cluster.KmeansReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 设置随机中心点的任务
public class CenterRandomAdapter {
    public static void createRandomCenter(String dataPath, String centerPath, int k){
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setInt("cluster.k", k);

        try {
            Job job = Job.getInstance(hadoopConfig, "random center task");

            job.setJarByClass(KmeansRun.class);
            job.setMapperClass(CenterRandomMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 输出为新计算得到的center，已存在则删除
            Path outPath = new Path(centerPath);
            outPath.getFileSystem(hadoopConfig).delete(outPath, true);

            //job执行作业时输入和输出文件的路径
            FileInputFormat.addInputPath(job, new Path(dataPath));
            FileOutputFormat.setOutputPath(job, new Path(centerPath));

            //执行job，直到完成
            job.waitForCompletion(true);
            System.out.println("random center task");
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
