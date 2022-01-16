package cn.edu.bupt.util;

import java.util.ArrayList;

// 计算工具类，两值距离，选择最近中心点等
public class CalUtil {

    // 计算两向量距离，欧式
    private static double calDistance(ArrayList<Double> element1, ArrayList<Double> element2){
        double disSum = 0;
        for(int i=0;i<element1.size();i++){
            disSum += (element1.get(i) - element2.get(i)) * (element1.get(i) - element2.get(i));
        }
        return Math.sqrt(disSum);
    }

    // 选择最近中心点,返回序号
    public static int selectNearestCenter(ArrayList<Double> element, ArrayList<ArrayList<Double>> centers){
        double minDis = 100000;
        int nearstIndex = 0;
        for(int i=0;i<centers.size();i++){
            ArrayList<Double> center = centers.get(i);
            double dis = calDistance(element, center);
            if(dis < minDis){
                minDis = dis;
                nearstIndex = i;
            }
        }
        return nearstIndex;
    }

    // 元素相加
    public static void addElement(ArrayList<Double> element1, ArrayList<Double> element2){
        for(int i=0;i<element1.size();i++) {
            element1.set(i, element1.get(i) + element2.get(i));
        }
    }

    // 计算新中心点
    public static void calCenter(int num, ArrayList<Double> element){
        for(int i=0;i<element.size();i++){
            element.set(i, element.get(i) / num);
        }
    }

    // 计算两次迭代的中心是否有变化，返回距离
    public static double calDistanceBetweenCenters(ArrayList<ArrayList<Double>>oldCenter, ArrayList<ArrayList<Double>>newCenter){
        // 因为data的读入顺序相同，所以最终收敛时聚类中心的顺序也相同
        // 只要遍历计算距离即可，不用考虑中心点本身顺序
        if(oldCenter.size() > newCenter.size())
            return 1000;
        double sum = 0;
        for(int i=0;i<oldCenter.size();i++){
            double singleDistance = calDistance(oldCenter.get(i), newCenter.get(i));
            sum += singleDistance;
        }
        return sum;
    }
}
