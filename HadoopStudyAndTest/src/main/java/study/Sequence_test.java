package study;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.*;
import org.mortbay.util.IO;

import java.io.IOException;
import java.io.InputStream;

public class Sequence_test {
    Configuration configuration;
    FileSystem fileSystem;
    @Before
    public void conn() throws IOException{
        configuration = new Configuration(true);
        fileSystem = FileSystem.get(configuration);
    }
    @After
    public void close() throws IOException{
        fileSystem.close();
    }
    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };
    @Test
    public void SequenceFileWriteDemo() throws IOException{
        String uri = "/numbers.seq";
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            // 创建Writer对象
            writer = SequenceFile.createWriter(fileSystem, configuration, path, key.getClass(), value.getClass());
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
    @Test
    public void SequenceFileReadDemo() throws IOException {
        String uri = "/NAIVE_BAYES_CLASSIFICATION_JOB_OUTPUT/part-r-00000";
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            // 创建Reader对象
            reader = new SequenceFile.Reader(fileSystem, path, configuration);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
            long position = reader.getPosition(); // 获取当前读取的字节对象
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : ""; // 检查是否遇到同步点
                System.out.printf("[%s%s]\t%s\n%s\n",
                        position, syncSeen, key, value);
                position = reader.getPosition(); // 更新当前位置，类似如while里i++
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    @Test
    public void evaluation() throws IOException {
        String uri = "/NAIVE_BAYES_CLASSIFICATION_JOB_OUTPUT/part-r-00000";
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            // 创建Reader对象
            reader = new SequenceFile.Reader(fileSystem, path, configuration);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
            long position = reader.getPosition(); // 获取当前读取的字节对象
            int correct = 0;
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : ""; // 检查是否遇到同步点
                System.out.printf("[%s%s]\t%s\n%s\n",
                        position, syncSeen, key, value);
                position = reader.getPosition(); // 更新当前位置，类似如while里i++
                if(key.toString().split("-")[0].equals(value.toString().split("@")[0])) {
                    correct += 1;
                }
            }
            System.out.println(correct);
        } finally {
            IOUtils.closeStream(reader);
        }
    }
    @Test
    public void MakePracticeSequenceFile() throws IOException{
        String inputPath = "/wordcount/input/I01001";
        String outputPath1 = "/input/practice/I01001.seq";
        String outputPath2 = "/input/test/I01001.seq";
        FileStatus[] files = fileSystem.listStatus(new Path(inputPath));
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer1 = SequenceFile.createWriter(fileSystem, configuration, new Path(outputPath1), key.getClass(), value.getClass());
        SequenceFile.Writer writer2 = SequenceFile.createWriter(fileSystem, configuration, new Path(outputPath2), key.getClass(), value.getClass());
        int partition =(int) (files.length * 0.6);
        for (int i = 0; i < partition; i ++) {
            key.set(files[i].getPath().getName());
            InputStream in = fileSystem.open(files[i].getPath());
            byte[] buffer = new byte[(int) files[i].getLen()];
            IOUtils.readFully(in, buffer, 0, buffer.length);
            value.set(buffer);
            IOUtils.closeStream(in);
            writer1.append(key, value);
        }
        IOUtils.closeStream(writer1);
        for (int i = partition; i < files.length; i ++) {
            key.set(files[i].getPath().getName());
            InputStream in = fileSystem.open(files[i].getPath());
            byte[] buffer = new byte[(int) files[i].getLen()];
            IOUtils.readFully(in, buffer, 0, buffer.length);
            value.set(buffer);
            IOUtils.closeStream(in);
            writer2.append(key, value);
        }
        IOUtils.closeStream(writer2);
    }
    @Test
    public void MakeTestSequenceFile() throws IOException{
        String inputPath = "/wordcount/input/I13000";
        String outputPath1 = "/input/practice/I13000.seq";
        String outputPath2 = "/input/test/I13000.seq";
        FileStatus[] files = fileSystem.listStatus(new Path(inputPath));
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer1 = SequenceFile.createWriter(fileSystem, configuration, new Path(outputPath1), key.getClass(), value.getClass());
        SequenceFile.Writer writer2 = SequenceFile.createWriter(fileSystem, configuration, new Path(outputPath2), key.getClass(), value.getClass());
        int partition =(int) (files.length * 0.6);
        for (int i = 0; i < partition; i ++) {
            key.set(files[i].getPath().getName());
            InputStream in = fileSystem.open(files[i].getPath());
            byte[] buffer = new byte[(int) files[i].getLen()];
            IOUtils.readFully(in, buffer, 0, buffer.length);
            value.set(buffer);
            IOUtils.closeStream(in);
            writer1.append(key, value);
        }
        IOUtils.closeStream(writer1);
        for (int i = partition; i < files.length; i ++) {
            key.set(files[i].getPath().getName());
            InputStream in = fileSystem.open(files[i].getPath());
            byte[] buffer = new byte[(int) files[i].getLen()];
            IOUtils.readFully(in, buffer, 0, buffer.length);
            value.set(buffer);
            IOUtils.closeStream(in);
            writer2.append(key, value);
        }
        IOUtils.closeStream(writer2);
    }
}
