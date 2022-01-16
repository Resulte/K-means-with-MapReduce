package cn.edu.bupt.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;

// 实现hdfs中中心点文件的读取和删除
// 中心点文件放在output中
public class DataUtil {
    public static final String HDFS_INPUT = "hdfs://localhost:9000/input"; // input地址
    public static final String HDFS_OUTPUT = "hdfs://localhost:9000/output"; // output地址

    // 切割字符串成double数组集合
    public static ArrayList<Double> splitStringIntoArray(String line){
        ArrayList<Double> center = new ArrayList<Double>();
        String[] lineContextArry = line.split(" ");
        for(String s:lineContextArry){
            Double c = Double.parseDouble(s);
            center.add(c);
        }
        return center;
    }

    // 将数组转为字符串形式
    public static String convertArrayIntoString(ArrayList<Double> element){
        StringBuffer sb = new StringBuffer();
        for(Double d:element){
            sb.append(d.toString()).append(" ");
        }
        return sb.substring(0, sb.length() - 1);
    }

    // 获取文件对应的hdfs系统下的linereader
    private static LineReader getLineReader(String filePath) throws IOException {
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = path.getFileSystem(conf);

        FSDataInputStream fsdis = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsdis, conf);

        return lineReader;
    }

    // 读入center
    private static void readCenterLines(LineReader lineReader, ArrayList<ArrayList<Double>> centers) throws IOException {
        Text line = new Text();
        // 每一行进行一次读取
        while (lineReader.readLine(line) > 0) {
            ArrayList<Double> center = splitStringIntoArray(line.toString().trim());
            centers.add(center);
        }
        lineReader.close();
    }

    // 读取中心点
    // 可能是文件夹，遍历读取
    public static ArrayList<ArrayList<Double>> readCenter(String centerPath) throws IOException {
        ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();

        Path path = new Path(centerPath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = path.getFileSystem(conf);

        if(fileSystem.isDirectory(path)){
            // 文件夹，遍历读取
            FileStatus[] listFile = fileSystem.listStatus(path);
            for (FileStatus fileStatus : listFile) {
                LineReader lineReader = getLineReader(fileStatus.getPath().toString());
                readCenterLines(lineReader, centers);
            }
        }else {
            // 普通文件，直接读取
            LineReader lineReader = getLineReader(centerPath);
            readCenterLines(lineReader, centers);
        }
        return centers;
    }

    // 删除中心点
    public static boolean deleteCenters(String centerPath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(centerPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        return fileSystem.delete(path, true);
    }

    // 利用new文件覆盖原文件
    public static void changeCenters(String centerPath, String newCenterPath, Configuration configuration) throws IOException {
        // 删除原center文件
        Path cPath = new Path(centerPath);
        Path ncPath = new Path(newCenterPath);
        ncPath.getFileSystem(configuration).delete(cPath, true);

        FileSystem fileSystem = FileSystem.get(configuration);
        RemoteIterator<LocatedFileStatus> sourceFiles = fileSystem.listFiles(ncPath, true);
        if(sourceFiles != null) {
            while(sourceFiles.hasNext()){
                FileUtil.copy(fileSystem, sourceFiles.next().getPath(), fileSystem, cPath, false, configuration);
            }
        }
    }
}
