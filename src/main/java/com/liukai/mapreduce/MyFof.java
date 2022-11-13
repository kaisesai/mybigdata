package com.liukai.mapreduce;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 好友推荐
 *
 * @author liukai
 */
public class MyFof {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 去读配置文件
        Configuration conf = new Configuration(true);
        // 设置本地执行
        conf.set("mapreduce.framework.name", "local");

        // 读取程序命令：/data/fof/input /data/fof/output
        String[] otherOptions = new GenericOptionsParser(conf, args).getRemainingArgs();

        // 创建 job
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyFof.class);
        job.setJobName("myFof");
        // TODO: 2022/10/30
        // job.setJar("/Users/liukai/IdeaProjects/liukai/mybigdata/target/mybigdata-1.0-SNAPSHOT.jar");
        // map join 右表 cache 到 maptask 出现的节点上
        // job.addCacheFile(new Path("/data/topn/dict/dict.txt").toUri());

        // mapTask
        // 输入格式化
        TextInputFormat.addInputPath(job, new Path(otherOptions[0]));

        Path outPath = new Path(otherOptions[1]);
        if (outPath.getFileSystem(conf).exists(outPath)) {
            outPath.getFileSystem(conf).delete(outPath, true);
        }
        // 输出格式化
        TextOutputFormat.setOutputPath(job, new Path(otherOptions[1]));


        // key,value
        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 分区
        // job.setPartitionerClass(TPartitioner.class);
        // 排序
        // job.setSortComparatorClass(TSortComparator.class);
        // combiner
        // job.setCombinerClass();

        // reduceTask
        job.setReducerClass(FReducer.class);

        // job.setNumReduceTasks(0);
        // 分组排序器
        // job.setGroupingComparatorClass(TGroupingComparator.class);

        // 等待计算完成
        job.waitForCompletion(true);

    }

    /**
     * map 映射器
     */
    @Slf4j
    private static class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // map 输出的 key
        private Text mKey = new Text();

        // map 输出的 value
        private IntWritable mValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // 读取输入文件中的数据

            // 数据：张三 李四 王五 赵六
            // 表示：张三有李四、王五好友，他们的关系是张三-李四、张三-王五、张三-赵六、李四-王五、李四-赵六、王五-赵六
            log.info("开始执行 map，读取到的值为：" + value.toString());

            String[] strs = StringUtils.split(value.toString(), ' ');

            // 穷举出一行记录中所有好友的关系
            for (int i = 1; i < strs.length; i++) {
                // 张三与其他人的关系为直接关系
                mKey.set(getFriendRelation(strs[0], strs[i]));
                // 关系：0，直接关系
                mValue.set(0);
                // 写操作
                context.write(mKey, mValue);
                for (int j = i+1; j < strs.length; j++) {
                    // 第二个人与其他人的关系
                    mKey.set(getFriendRelation(strs[i], strs[j]));
                    // 关系：1，间接关系
                    mValue.set(1);
                    // 写操作
                    context.write(mKey, mValue);
                }
            }

        }

        private String getFriendRelation(String first, String second) {
            if (first.compareTo(second) > 0) {
                return first + "-" + second;
            } else {
                return second + "-" + first;
            }
        }
    }


    /**
     * reduce 计算器
     */
    @Slf4j
    private static class FReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable rValue = new IntWritable();


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            // 读取好友的关系
            // 张三-李四 1
            // 张三-李四 0
            // 张三-李四 1
            // 张三-李四 0
            // 只要是出现了 0 就表示它们的关系是直接关系，直接过滤不输出
            System.out.println("执行 TReducer reduce");

            // 遍历过程中，key 在不断的变化
            int flg = 0;
            int sum = 0;
            for (IntWritable value : values) {
                if(value.get() == 0){
                    // value 值为 0 说明是直接关系，不记录数据
                    // 输出
                    flg = 1;
                }
                sum += value.get();
                log.info("reduce task 遍历 values，key: {}, value: {}", key, value.get());
            }

            if(flg == 0){
                rValue.set(sum);
                context.write(key, rValue);
                log.info("reduce task 输出 key:{} ，value: {}", key, sum);
            }

        }

    }
}
