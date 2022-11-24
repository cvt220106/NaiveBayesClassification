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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Const;
import java.io.IOException;


public class TypeNumCountJob extends Configured implements Tool{
    // 将训练集中各类别的文档数统计出来
    public static class TypeNumCountMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        private Text TypeName = new Text();
        private IntWritable TypeCount = new IntWritable(1);

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            /**
             *input: key:I01001-478471newsML.txt value:478471newsML.txt文档的内容
             *output：key:I01001(TypeName)  value:1
             */
            System.out.println("start TypeNumCountMapper map()");
            String[] keyName = key.toString().split("-");
            this.TypeName.set(keyName[0]);
            this.TypeCount.set(1);
            context.write(this.TypeName, this.TypeCount);
        }
    }

    public static class TypeNumCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text Type = new Text();
        private IntWritable TypeSumNum = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            /**
            *由于设置了job.setCombinerClass(TypeCountReducer.class);
            *因此传入的value为数组{}
            *input: key:I01001(TypeName) value: Iterable<IntWritable>{.....}
            *output: key:I01001(TypeName) value:sum of Iterable<IntWritable>{.....}
            */
            System.out.println("start TypeNumCountReducer reduce()");
            int totalSum = 0;
            for (IntWritable value : values) {
                totalSum += value.get();
            }
            this.Type.set(key);
            this.TypeSumNum.set(totalSum);
            context.write(this.Type, this.TypeSumNum);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("开始对 GetDocCountFromDocTypeJob 进行配置");

        Configuration conf = new Configuration();

        // 如果输出目录存在，则先删除输出目录
        Path outputPath = new Path(Const.GET_TYPE_NUM_COUNT_JOB_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "GetDocCountFromDocTypeJob");

        job.setJarByClass(TypeNumCountJob.class);
        job.setMapperClass(TypeNumCountMapper.class);
        job.setCombinerClass(TypeNumCountReducer.class);
        job.setReducerClass(TypeNumCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Const.GET_TRAIN_DATA_SEQUENCE_FILE_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Const.GET_TYPE_NUM_COUNT_JOB_OUTPUT_PATH));

        System.out.println("完成配置，开始执行 TypeNumCountJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new TypeNumCountJob(), args);
        System.exit(res);
    }
}

