package study;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

// 在Windows本地进行的数据集随即分割
public class RandomSplit {
    public static void main(String[] args) throws IOException {
        Split("I01001");
        System.out.println();
        Split("I13000");
    }

    public static void Split(String dirName) throws IOException {
        // 指定训练集和测试集的输出目录
        String trainDir = "F:\\opt\\module\\TRAIN_DATA_FILE\\" + dirName + "\\";
        String testDir =  "F:\\opt\\module\\TEST_DATA_FILE\\" + dirName + "\\";
        // 指定文件集的输入路径
        File srcDir = new File( "F:\\opt\\module\\NBCorpus\\" + dirName);
        File[] files = srcDir.listFiles();
        boolean[] flag = new boolean[files.length];
        int trainNum = (int) ((int) files.length * 0.6);
        Random random = new Random();
        int count = 0;
        // 随机选择数据集的60%作为训练集
        while (count < trainNum) {
            int num = random.nextInt(files.length);
            System.out.print(num + " "); // 通过输出看看随机过程
            if (flag[num]) {
                continue; // 被选中过直接重新选择随机数
            }
            flag[num] = true;
            File file = files[num];
            String src = file.getAbsolutePath();
            String des = trainDir;
            FileCopy(src, des);
            count ++;
        }
        //剩下的40%放入测试集
        for (int i = 0; i < files.length; i ++) {
            if (flag[i]) {
                continue;
            }
            File file = files[i];
            String src = file.getAbsolutePath();
            String des = testDir;
            FileCopy(src, des);
        }
    }

    public static void FileCopy(String src, String des) throws IOException {
        File srcFile = new File(src);
        File destFile = new File(des + srcFile.getName());
        if (! destFile.exists()) {
            destFile.createNewFile();
        }
        FileReader fileReader = new FileReader(srcFile);
        FileWriter fileWriter = new FileWriter(destFile);

        char[] chs = new char[1024];
        int len = 0;
        while ((len = fileReader.read(chs)) != -1) {
            fileWriter.write(chs, 0, len);
        }

        fileReader.close();
        fileWriter.close();
    }

 }
