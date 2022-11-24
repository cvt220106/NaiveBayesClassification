package ProjectJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Const;

import java.io.IOException;
import java.util.regex.Pattern;
public class WordInTypeCountJob extends Configured implements Tool{
    public static final Logger log = LoggerFactory.getLogger(WordInTypeCountJob.class);

    public static class WordInTypeCountMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        // 类型-单词
        private Text word =  new Text();
        // 出现次数
        private IntWritable SinglewordCount = new IntWritable(1);
        // 匹配模板，用于找出文档中所需的英文单词
        private static final Pattern EnglishWordRegex = Pattern.compile("^[A-Za-z]{2,}$");

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException{
            /**
            input: key:I01001-478471newsML.txt value:478471newsML.txt文档的内容
            output：key:I01001(TypeName)-word  value:1
            */
            System.out.println("start WordInTypeCountMapper map()");
            String TypeName = key.toString().split("-")[0];
            // 将Sequence File中的byte形式的value内容转为String
            String content = new String(value.getBytes());

            // 通过split将文件内容分为一个个单词构建为单词列表
            String[] wordList = content.split("\\s+");
            // 再通过regex的pattern匹配以及排除掉一些无用的单词如a，and等，从而得到想要的单词并进行处理
            for (String word : wordList) {
                if (EnglishWordRegex.matcher(word).find() && !Const.STOP_WORDS_LIST.contains(word)) {
                    this.word.set(TypeName + "-" + word);
                    context.write(this.word, this.SinglewordCount);
                } else if (word.contains(".")) { // 存在一行中有两个单词之间一个.的情况
                    for (String subword : word.split(".")) { // 将改行split后再判断是否为无效词
                        if (!Const.STOP_WORDS_LIST.contains(subword)) {
                            this.word.set(TypeName + "-" + subword);
                            context.write(this.word, this.SinglewordCount);
                        }
                    }
                } else {
                    log.debug("过滤的词" + word);
                }
            }
        }
    }

    public static class WordInTypeCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 单词在对应类别文档中出现的总次数
        private IntWritable SingleWordSumCount = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
            /**
             input: key:I01001(TypeName)-word value:{......}
             output：key:I01001(TypeName)-word  value:SingleWordSumCount
             */
            System.out.println("start WordInTypeCountReducer reduce()");
            int totalSum = 0;
            for (IntWritable value : values) {
                totalSum += value.get();
            }
            this.SingleWordSumCount.set(totalSum);
            context.write(key, this.SingleWordSumCount);
        }
    }

    @Override
    public int run(String[] strings) throws Exception{
        System.out.println("开始对 WordInTypeCountJob 进行配置");

        Configuration conf = new Configuration();

        // 如果输出目录存在，则先删除输出目录
        Path outputPath = new Path(Const.GET_WORD_IN_TYPE_COUNT_JOB_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "WordInTypeCountJob");

        job.setJarByClass(WordInTypeCountJob.class);
        job.setMapperClass(WordInTypeCountMapper.class);
        job.setCombinerClass(WordInTypeCountReducer.class);
        job.setReducerClass(WordInTypeCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Const.GET_TRAIN_DATA_SEQUENCE_FILE_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Const.GET_WORD_IN_TYPE_COUNT_JOB_OUTPUT_PATH));

        System.out.println("完成配置，开始执行 WordInTypeCountJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordInTypeCountJob(), args);
        System.out.println("WordInTypeCountJob 运行结束, 已计算每个文档类型中每个单词出现的次数");
        System.exit(res);
    }
}
