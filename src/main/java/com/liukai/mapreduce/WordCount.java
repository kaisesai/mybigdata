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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * 单词统计
 *
 * @author liukai
 */
public class WordCount {

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {

    // args = [-D, a=123, input, outputargs, -Dbbb=333]
    System.out.println("args = " + Arrays.toString(args));

    System.out.println("===========日志分割线========");
    Properties properties = System.getProperties();
    /*
    ...
    entry = os.name=Mac OS X
    entry = java.vm.specification.version=11
    entry = sun.java.launcher=SUN_STANDARD
    entry = sun.boot.library.path=/Users/liukai/app/jdk-11.0.14.1+1/Contents/Home/lib
    entry = sun.java.command=com.liukai.mapreduce.WordCount -D a=123 input outputargs1 -Dbbb=333 -Dmapreduce.framework.name=yarn
    entry = user.home=/Users/liukai
    entry = java.version.date=2022-02-08
    entry = java.home=/Users/liukai/app/jdk-11.0.14.1+1/Contents/Home
    entry = file.separator=/
    entry = os.arch=x86_64
    entry = java.library.path=/Users/liukai/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:.
    entry = java.vm.info=mixed mode
    entry = java.class.version=55.0
    ...
     */
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      System.out.println("entry = " + entry);
    }
    System.out.println("===========日志分割线========");

    // 配置文件
    Configuration conf = new Configuration(true);
    // conf.set("mapreduce.framework.name", "local");

    // 通用的命令参数解析器
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    // 获取解析系统命令（-D 之类系统属性参数）的剩余的命令，比如用户的普通参数
    String[] remainingArgs = parser.getRemainingArgs();

    // 创建一个 job
    Job job = Job.getInstance(conf, "WordCountJob");

    // 已上传 jar 包集群的方式进行运行，需要设置加载的 jar 包路径
    setJobJarForUploadJarToNodeGroup(job);

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
    FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  private static void setJobJarForUploadJarToNodeGroup(Job job) {
    // 设置 jar 包，用于在将 jar 包上传到集群，以便让集群的节点可以加载对应的 jar 中的类
    job.setJar("/Users/liukai/IdeaProjects/liukai/mybigdata/target/mybigdata-1.0-SNAPSHOT.jar");
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
