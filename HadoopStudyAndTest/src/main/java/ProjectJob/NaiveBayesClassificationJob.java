package ProjectJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Const;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
/**
 * 计算训练集的先验概率、条件概率(setup中进行)，并通过MapReduce人任务计算测试集的每个文档分成每一类的概率
 * 读取InitSequenceFileJob生成的测试集的sequence_file计算测试集的每个文档分成每一类的概率
 */
public class NaiveBayesClassificationJob extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(NaiveBayesClassificationJob.class);
    /**
     * 种类列表
     */
    private static String[] TypeList;

    /**
     * 每个类别中每个单词出现的次数
     */
    private static Map<String, Integer> WordInTypeCountMap = new HashMap<>();

    /**
     * 每个类别中所有单词数
     */
    private static Map<String, Integer> WordSumInTypeCountMap = new HashMap<>();

    /**
     * 每个文档ci的先验概率P(ci)
     */
    private static Map<String, Double> DocTypePriorProbabilityMap = new HashMap<>();

    /**
     * 每个单词wi的条件概率P(wi|ci)
     */
    private static Map<String, Double> WordConditionalProbabilityMap = new HashMap<>();

    /**
     * 有效单词的正则匹配表达式
     */
    private static final Pattern EnglishWordRegex = Pattern.compile("^[A-Za-z]{2,}$");

    public static class NaiveBayesClassificationMapper extends Mapper<Text, BytesWritable, Text, Text> {
        // 测试集中单词的条件概率
        Text conditionProbability = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path TypeNumCountPath = new Path(Const.GET_TYPE_NUM_COUNT_JOB_OUTPUT_PATH +  Const.HADOOP_DEFAULT_OUTPUT_FILE_NAME);
            Path WordInTypeCountPath = new Path(Const.GET_WORD_IN_TYPE_COUNT_JOB_OUTPUT_PATH + Const.HADOOP_DEFAULT_OUTPUT_FILE_NAME);
            Path WordSumInTypeCountPath = new Path(Const.GET_WORD_SUM_IN_TYPE_COUNT_JOB_OUTPUT_PATH + Const.HADOOP_DEFAULT_OUTPUT_FILE_NAME);

            // 获取文档类型列表
            conf.set("INPUT_PATH", Const.GET_TEST_DATA_INPUT_PATH);
            conf.set("OUTPUT_PATH", Const.GET_TEST_DATA_SEQUENCE_FILE_PATH);
            conf.set("TYPE_LIST", Const.TYPE_LIST);
            TypeList = conf.get("TYPE_LIST").split("-");

            FileSystem fs = FileSystem.get(conf);
            //读取SequenceFile
            SequenceFile.Reader reader = null;
            double totalDocCount = 0;
            Map<String, Integer> TypeNumCountMap = new HashMap<String, Integer>(5);
            try {
                // 读取前面得到的文档类型与文档数目组成的sequence file写入map，并同时计算总文档数
                SequenceFile.Reader.Option option = SequenceFile.Reader.file(TypeNumCountPath);
                reader = new SequenceFile.Reader(conf, option);
                Text key = new Text(); // TypeName
                IntWritable value = new IntWritable(); // 对应类型文档数
                while (reader.next(key, value)) {
                    TypeNumCountMap.put(key.toString(), value.get());
                    totalDocCount += value.get();
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            } finally {
                // 任何情况下都要保证关闭reader流
                IOUtils.closeStream(reader);
            }
            // 计算文档ci的先验概率：P(ci)=类型ci的文档数/总文档数
            double TotalDocCount = totalDocCount;
            TypeNumCountMap.forEach((TypeName, TypeDocNum) -> {
                double priorProbability = TypeDocNum / TotalDocCount;
                DocTypePriorProbabilityMap.put(TypeName, priorProbability);
            });

            // 取出sequence file中前面生成的每个文档类型中各个单词的出现次数
            try {
                SequenceFile.Reader.Option option = SequenceFile.Reader.file(WordInTypeCountPath);
                reader = new SequenceFile.Reader(conf, option);
                Text key = new Text(); // TypeName-Word
                IntWritable value = new IntWritable(); // 对应类型文档中对应word的出现次数
                while (reader.next(key, value)) {
                    WordInTypeCountMap.put(key.toString(), value.get());
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            } finally {
                IOUtils.closeStream(reader);
            }

            // 取出sequence file中前面生成的每个文档类型中所有单词数
            try {
                SequenceFile.Reader.Option option = SequenceFile.Reader.file(WordSumInTypeCountPath);
                reader = new SequenceFile.Reader(conf, option);
                Text key = new Text(); // TypeName
                IntWritable value = new IntWritable(); // 对应类型文档中的单词总数
                while (reader.next(key, value)) {
                    WordSumInTypeCountMap.put(key.toString(), value.get());
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            } finally {
                IOUtils.closeStream(reader);
            }

            // 计算每个单词的条件概率+加入拉普拉斯平滑
            WordInTypeCountMap.forEach((key, value) -> {
                String TypeName = key.split("-")[0];
                double probability = (value.doubleValue() + 1.0)
                        / (WordSumInTypeCountMap.get(TypeName).doubleValue() + (double)WordInTypeCountMap.size());
                WordConditionalProbabilityMap.put(key, probability);
            });
        }

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            /**
             *input: key:I01001-478471newsML.txt value:478471newsML.txt文档的内容
             *destination: 计算文档d为类别ci的条件概率：P(d|ci)正相关于∏P(wi|ci)*P(ci)
             * output：key:I01001-478471newsML.txt  value:TypeName@Probability
             */
            System.out.println("start NaiveBayesClassificationMapper map()");
            String content = new String(value.getBytes());
            String[] wordList = content.split("\\s+");
            for (String TypeName : TypeList) {
                double conditionProbability = 0;
                // 计算每个单词在对应文档中的条件概率对数之和
                for (String word : wordList) {
                    if (EnglishWordRegex.matcher(word).find() && !Const.STOP_WORDS_LIST.contains(word)) {
                        String wordKey = TypeName + "-" + word;
                        if (WordConditionalProbabilityMap.containsKey(wordKey)) {
                            conditionProbability += Math.log10(WordConditionalProbabilityMap.get(wordKey));
                        } else {
                            conditionProbability += Math.log10(1.0 / (WordSumInTypeCountMap.get(TypeName).doubleValue() + (double)WordInTypeCountMap.size()));
                        }
                    } else {
                        log.debug("过滤无用词：" + word);
                    }
                }
                // 最后加上文档ci的先验概率
                conditionProbability += Math.log10(DocTypePriorProbabilityMap.get(TypeName));
                /**
                 * 此处不再与之前一般使用“-”的原因在于概率值log计算后为负数，转化为text后也存在一个“-”，便会导致后续处理出错
                 * 为了避免这一情况的发生，此处改用“@”作为分隔符！
                 */
                this.conditionProbability.set(TypeName + "@" + conditionProbability);
                context.write(key, this.conditionProbability);
            }
        }
    }

    public static class NaiveBayesClassificationReducer extends Reducer<Text, Text, Text, Text> {
        // 测试集中文档被分为ci类别的概率
        Text TypePredictResult = new Text();

        @Override
        // 通过reduce进行类别概率的比较从而预测
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /**
             *input: key:I01001-478471newsML.txt  value:{TypeName@Probability...}
             * output：key:I01001-478471newsML.txt  value:PredictTypeName@maxProbability
             */
            // 最大概率初始设置为负无穷
            System.out.println("start NaiveBayesClassificationReducer reduce()");
            double maxProbability = Double.NEGATIVE_INFINITY;
            String predictTypeName = "";
            for (Text value : values) {
                double predictProbability = Double.parseDouble(value.toString().split("@")[1]);
                if (predictProbability > maxProbability) {
                    maxProbability = predictProbability;
                    predictTypeName = value.toString().split("@")[0];
                }
            }
            /**
             * 此处使用@作为分隔符的原因同上！
             */
            this.TypePredictResult.set(predictTypeName + "@" + maxProbability);
            context.write(key, this.TypePredictResult);
        }
    }

    @Override
    public int run(String[] strings) throws Exception{
        System.out.println("开始对 GetNaiveBayesResultJob 进行配置");

        Configuration conf = new Configuration();

        // 如果输出目录存在，则先删除输出目录
        Path outputPath = new Path(Const.GET_NAIVE_BAYES_CLASSIFICATION_JOB_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "NaiveBayesClassificationJob");

        job.setJarByClass(NaiveBayesClassificationJob.class);
        job.setMapperClass(NaiveBayesClassificationMapper.class);
        job.setCombinerClass(NaiveBayesClassificationReducer.class);
        job.setReducerClass(NaiveBayesClassificationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Const.GET_TEST_DATA_SEQUENCE_FILE_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Const.GET_NAIVE_BAYES_CLASSIFICATION_JOB_OUTPUT_PATH));

        System.out.println("完成配置，开始执行 GetNaiveBayesResultJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new NaiveBayesClassificationJob(), args);
        System.out.println("NaiveBayesClassificationJob 运行结束");
        System.exit(res);
    }
}
