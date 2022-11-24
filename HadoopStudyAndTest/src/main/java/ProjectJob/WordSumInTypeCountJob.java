package ProjectJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Const;
import java.io.IOException;

// 获取总单词数在此文档类型中
public class WordSumInTypeCountJob extends Configured implements Tool{
    public static class WordSumInTypeCountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable SingleWordNumInType = new IntWritable(0);
        private int count = 0;

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            /**
            input: key:I01001(TypeName)-word   value:IntWritable--指明word在对应TypeName文档中出现的次数
            output：key:I01001(TypeName)        value:IntWritable
             */
            System.out.println("start WordSumInTypeCountMapper map()");
            String TypeName = key.toString().split("-")[0];
            this.word.set(TypeName);
            this.SingleWordNumInType .set(value.get());
            context.write(this.word, this.SingleWordNumInType );
        }
    }

    public static class WordSumInTypeCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable TotalWordNumInType = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /**
            input: key:I01001(TypeName)   value:Iterable<IntWritable> {......}
            output：key:I01001(TypeName)   value:sum of {......}
             */
            System.out.println("start WordSumInTypeCountReducer reduce()");
            int totalWordNum = 0;
            for (IntWritable value : values) {
                totalWordNum += value.get();
            }
            this.TotalWordNumInType.set(totalWordNum);
            context.write(key, TotalWordNumInType);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("开始对 WordSumInTypeCountJob 进行配置");

        Configuration conf = new Configuration();

        // 如果输出目录存在，则先删除输出目录
        Path outputPath = new Path(Const.GET_WORD_SUM_IN_TYPE_COUNT_JOB_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "WordSumTypeCountJob");

        job.setJarByClass(WordSumInTypeCountJob.class);
        job.setMapperClass(WordSumInTypeCountMapper.class);
        job.setCombinerClass(WordSumInTypeCountReducer.class);
        job.setReducerClass(WordSumInTypeCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Const.GET_WORD_IN_TYPE_COUNT_JOB_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Const.GET_WORD_SUM_IN_TYPE_COUNT_JOB_OUTPUT_PATH));

        System.out.println("完成配置， 开始执行 WordSumInTypeCountJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordSumInTypeCountJob(), args);
        System.out.println("WordSumInTypeCountJob 运行结束，已计算每个类型文档中的单词总数");
    }
}
