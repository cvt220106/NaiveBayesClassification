package study;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class hadoop_test {
    Configuration configuration;
    FileSystem fileSystem;

    @Before
    public void conn() throws IOException {
        //1.配置
        configuration = new Configuration(true);
        //2.文件系统
        fileSystem = FileSystem.get(configuration);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void mkdir() throws IOException {
        //2.文件系统
        Path path = new Path("/wordcount/input");
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        fileSystem.mkdirs(path);

    }
    @Test
    public void fsCat() throws IOException{
        String uri = "/wordcount/input/I01001/478471newsML.txt";
        InputStream in = null;
        try {
            in = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(in, System.out,4096,false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
    @Test
    public void listStatus() throws IOException{
        String uri = "/wordcount/input/I01001";
        Path path = new Path(uri);
        FileStatus[] status = fileSystem.listStatus(path);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for(Path p : listedPaths) {
            System.out.println(p);
        }
    }
}
