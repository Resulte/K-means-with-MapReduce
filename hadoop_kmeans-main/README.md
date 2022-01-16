# hadoop_kmeans
kmeans achieved by hadoop apis</br>

基于MapReduce的KMeans算法流程如下：</br>
1. 随机分配簇，初始化中心点，存入HDFS。
2. Mapper中读取数据文件中的每条数据并与中心点进行距离计算，输出key为最近的中心点序号。
3. Reducer中进行归并，计算新的中心点，存入新的中心文件。
4. 判断停机条件，不满足则复制新的中心文件到原中心文件，重复2，3步骤。
5. 输出聚类结果，包括数据点信息与对应簇序号。

项目说明：[基于MapReduce实现的Kmeans算法](https://blog.csdn.net/qq_41733192/article/details/118662171)
