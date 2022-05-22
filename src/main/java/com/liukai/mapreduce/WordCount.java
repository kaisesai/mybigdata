package com.liukai.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 单词统计
 *
 * @author liukai
 */
public class WordCount {

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {

    // 配置文件
    Configuration conf = new Configuration(true);
    // 创建一个 job
    Job job = Job.getInstance(conf, "WordCountJob");
    // jar 包类型
    job.setJarByClass(WordCount.class);
    // map 实现类
    job.setMapperClass(TokenizerMapper.class);
    // 联合类
    job.setCombinerClass(IntSumReducer.class);
    // reduce 实现类
    job.setReducerClass(IntSumReducer.class);
    // 输出的 key
    job.setOutputKeyClass(Text.class);
    // 输出的 value
    job.setOutputValueClass(IntWritable.class);

    // 输入目录：/user/liukai/
    // 输出目录：

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /** mapper 映射实现类 */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  /** reduce 实现类 */
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
}
