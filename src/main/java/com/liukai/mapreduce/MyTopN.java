package com.liukai.mapreduce;

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 求一个每个月气温最高的两天的数据
 *
 * @author liukai
 */
public class MyTopN {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 去读配置文件
        Configuration conf = new Configuration(true);
        // 设置本地执行
        conf.set("mapreduce.framework.name", "local");

        // 读取程序命令：/data/topn/input /data/topn/output
        String[] otherOptions = new GenericOptionsParser(conf, args).getRemainingArgs();

        // 创建 job
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyTopN.class);
        job.setJobName("myTopN");
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
        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 分区
        job.setPartitionerClass(TPartitioner.class);
        // 排序
        job.setSortComparatorClass(TSortComparator.class);
        // combiner
        // job.setCombinerClass();

        // reduceTask
        // 分组排序器
        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReducer.class);

        // 等待计算完成
        job.waitForCompletion(true);

    }

    /**
     * map 映射器
     */
    @Slf4j
    private static class TMapper extends Mapper<LongWritable, Text, TKey, IntWritable> {

        // map 输出的 key
        private TKey mKey = new TKey();

        // map 输出的 value
        private IntWritable mValue = new IntWritable();


        /**
         * 城市编码字段
         */
        private final Map<String, String> dictMap = Maps.newHashMap();

        @Override
        protected void setup(Mapper<LongWritable, Text, TKey, IntWritable>.Context context) throws IOException, InterruptedException {

            // 从本地的缓存文件中读取文件
            // URI[] cacheFiles = context.getCacheFiles();
            // Path path = new Path(cacheFiles[0].getPath());
            // File file = new File(path.getName());
            File file = new File("/Users/liukai/IdeaProjects/liukai/mybigdata/data/dict.txt");

            List<String> lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
            lines.forEach(line -> {
                String[] split = line.split(" ");
                dictMap.put(split[0], split[1]);
            });
            log.info("map 阶段初始化 dictMap: {}", dictMap);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TKey, IntWritable>.Context context) throws IOException, InterruptedException {
            // 读取输入文件中的数据

            System.out.println("开始执行 map，读取到的值为：" + value.toString());

            String[] strs = StringUtils.split(value.toString(), '\t');

            SimpleDateFormat sdf = new SimpleDateFormat("yyy-MM-dd");

            try {
                // 解析年月日
                Date date = sdf.parse(strs[0]);

                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);

                // 为 TKey 设置年月日
                mKey.setYear(calendar.get(Calendar.YEAR));
                mKey.setMonth(calendar.get(Calendar.MONTH) + 1);
                mKey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
                // 设置城市
                mKey.setLocation(dictMap.get(strs[1]));

                // 为 TKey 设置温度
                int temperature = Integer.parseInt(strs[2]);
                mKey.setTemperature(temperature);

                // 为 myValue 设置值
                mValue.set(temperature);

                // 将 key、value 写入上线
                context.write(mKey, mValue);

            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * 自定义的 key，同时实现了MapReduce 的序列化方法，实现写出与读入的方法
     */
    @Data
    private static class TKey implements WritableComparable<TKey> {
        private int year;
        private int month;
        private int day;
        private int temperature;

        private String location;

        /**
         * 通用的比较方法，年月日升序
         *
         * @param o the object to be compared.
         * @return
         */
        @Override
        public int compareTo(TKey o) {

            System.out.println("执行 TKey 排序器, o: " + o);

            // 年
            int yearCompare = Integer.compare(year, o.year);
            if (yearCompare != 0) {
                return yearCompare;
            }

            // 月
            int monthCompare = Integer.compare(month, o.getMonth());
            if (monthCompare != 0) {
                return monthCompare;
            }

            // 日
            return Integer.compare(day, o.getDay());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(year);
            out.writeInt(month);
            out.writeInt(day);
            out.writeInt(temperature);
            out.writeUTF(location);
            System.out.println("执行 TKey write，当前 tkey: " + this);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.setYear(in.readInt());
            this.setMonth(in.readInt());
            this.setDay(in.readInt());
            this.setTemperature(in.readInt());
            this.setLocation(in.readUTF());
            System.out.println("执行 TKey readFields，当前 tkey: " + this);
        }
    }

    /**
     * 分区排序器
     */
    private static class TPartitioner extends Partitioner<TKey, IntWritable> {

        /**
         * 获取分区号，根据 key 中的年来计算分区数据
         *
         * @param tKey          the key to be partioned.
         * @param intWritable   the entry value.
         * @param numPartitions the total number of partitions.
         * @return
         */
        @Override
        public int getPartition(TKey tKey, IntWritable intWritable, int numPartitions) {

            System.out.println("执行 TPartitioner getPartition");

            // 相同的 key 为一个分区
            // todo 注意数据倾斜
            return tKey.getYear() % numPartitions;
        }
    }

    /**
     * map 阶段自定义排序器，区别于 key 的排序器
     */
    private static class TSortComparator extends WritableComparator {

        public TSortComparator() {
            super(TKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {

            System.out.println("执行 TSortComparator compare");

            TKey t1 = (TKey) a;
            TKey t2 = (TKey) b;

            // 按照年月日排序升序，降序
            // 年
            int yearCompare = Integer.compare(t1.getYear(), t2.getYear());
            if (yearCompare != 0) {
                return yearCompare;
            }

            // 月
            int monthCompare = Integer.compare(t1.getMonth(), t2.getMonth());
            if (monthCompare != 0) {
                return monthCompare;
            }

            // 日
            int dayCompare = Integer.compare(t1.getDay(), t2.getDay());
            if (dayCompare != 0) {
                return dayCompare;
            }

            // 按照温度降序
            return -Integer.compare(t1.getTemperature(), t2.getTemperature());
        }
    }

    /**
     * reduce 阶段的分组排序
     */
    private static class TGroupingComparator extends WritableComparator {

        public TGroupingComparator() {
            super(TKey.class, true);
        }


        /**
         * 将年月作为一个分组
         *
         * @param a
         * @param b
         * @return
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {


            System.out.println("执行 TGroupingComparator compare");

            TKey t1 = (TKey) a;
            TKey t2 = (TKey) b;

            // 按照年月分组排序
            // 年
            int yearCompare = Integer.compare(t1.getYear(), t2.getYear());
            if (yearCompare != 0) {
                return yearCompare;
            }
            return Integer.compare(t1.getMonth(), t2.getMonth());
        }
    }

    /**
     * reduce 计算器
     */
    private static class TReducer extends Reducer<TKey, IntWritable, Text, IntWritable> {

        private Text rKey = new Text();
        private IntWritable rValue = new IntWritable();


        @Override
        protected void reduce(TKey key, Iterable<IntWritable> values, Reducer<TKey, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            System.out.println("执行 TReducer reduce");


            // 1970-6-4 33   33
            // 1970-6-4 32   32
            // 1970-6-22 31   31
            // 1970-6-4 22   22


            // 对 value 迭代器进行迭代，可以也会发生变换
            Iterator<IntWritable> iterator = values.iterator();

            int flg = 0;
            int day = 0;
            while (iterator.hasNext()) {
                IntWritable val = iterator.next();
                System.out.println("迭代 reduce 中的 values 的迭代器，key：" + key);

                if (flg == 0) {
                    doExtracted(key, context);
                    flg++;
                    // 记录第一次遍历的日
                    day = key.getDay();
                }

                if (flg != 0 && day != key.getDay()) {
                    // 说明不是第一次遍历
                    doExtracted(key, context);
                    break;
                }

            }

        }

        private void doExtracted(TKey key, Reducer<TKey, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "@"+ key.getLocation());
            rValue.set(key.getTemperature());
            // 将 key、value 写入 context
            context.write(rKey, rValue);
        }
    }
}
