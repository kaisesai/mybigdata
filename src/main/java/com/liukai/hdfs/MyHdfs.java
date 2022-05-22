package com.liukai.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

@Slf4j
public class MyHdfs {

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void init() throws IOException {
    // 配置文件
    conf = new Configuration(true);
    // 文件系统
    fs = FileSystem.get(conf);
  }

  @Test
  public void mkdir() throws IOException {
    // 创建目录
    Path path = new Path("/msh01");
    if (fs.exists(path)) {
      log.info("文件目录已经存在：{}", path);
      fs.delete(path, true);
      log.info("已删除文件：{}", path);
    }
    log.info("准备创建文件：{}", path);
    fs.mkdirs(path);
    log.info("已创建文件:{}", path);
  }

  @Test
  public void delete() throws IOException {
    // 创建目录
    // Path path = new Path("/msh01");
    // Path path = new Path("/user/liukai/input");
    Path path = new Path("/user/liukai/output");
    // Path path = new Path("/tmp");
    if (fs.exists(path)) {
      log.info("文件目录已经存在：{}", path);
      fs.delete(path, true);
      log.info("已删除文件：{}", path);
    }else{
      log.info("文件目录不存在：{}", path);
    }
  }

  @Test
  public void upload() throws IOException {
    BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/liukai/data.txt"));
    Path path = new Path("/msb01/out.txt");
    FSDataOutputStream outputStream = fs.create(path, true);

    IOUtils.copyBytes(in, outputStream, conf, true);
    log.info("上传成功！");
  }

  @Test
  public void blocks() throws IOException {
    // 读取文件信息
    Path path = new Path("/user/liukai/data.txt");
    FileStatus fileStatus = fs.getFileStatus(path);
    System.out.println("fileStatus = " + fileStatus);

    // 读取文件块
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    for (BlockLocation blockLocation : blockLocations) {
      System.out.println("blockLocation = " + blockLocation);
    }

    /*
    fileStatus = HdfsNamedFileStatus{path=hdfs://localhost:9000/user/liukai/data.txt; isDirectory=false; length=1888895; replication=1; blocksize=1048576; modification_time=1649582903603; access_time=1650718991903; owner=liukai; group=supergroup; permission=rw-r--r--; isSymlink=false; hasAcl=false; isEncrypted=false; isErasureCoded=false}
    blockLocation = 0,1048576,192.168.1.102
    blockLocation = 1048576,840319,192.168.1.102
     */
    FSDataInputStream in = fs.open(path);
    // 通过 seek 方法可以直接跳转到指定的偏移量来读取文件数据，这就是 hdfs 的计算向数据移动，只读取自己关注的数据，具有距离的概念
    in.seek(1048576);

    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());
    System.out.println((char) in.readByte());

    /*
    fileStatus = HdfsNamedFileStatus{path=hdfs://localhost:9000/user/liukai/data.txt; isDirectory=false; length=1888895; replication=1; blocksize=1048576; modification_time=1649582903603; access_time=1651579272126; owner=liukai; group=supergroup; permission=rw-r--r--; isSymlink=false; hasAcl=false; isEncrypted=false; isErasureCoded=false}
    blockLocation = 0,1048576,192.168.1.102
    blockLocation = 1048576,840319,192.168.1.102
    5
    7
    7
    3


    h
    e
    l
    l
     */

  }

  @After
  public void close() throws IOException {
    // 关闭文件
    fs.close();
  }
}
