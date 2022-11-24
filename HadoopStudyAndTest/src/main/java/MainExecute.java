import ProjectJob.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import utils.Const;
public class MainExecute {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /**
         * 将训练集的多个文件合并为一个Sequence file
         */
        System.out.println("开始将训练集的多个文件合并为一个Sequence file");
        conf.set("INPUT_PATH", Const.GET_TRAIN_DATA_INPUT_PATH);
        conf.set("OUTPUT_PATH", Const.GET_TRAIN_DATA_SEQUENCE_FILE_PATH);
        InitSequenceFileJob initSequenceFileJob = new InitSequenceFileJob();
        ToolRunner.run(conf, initSequenceFileJob , args);
        System.out.println("训练集文件 InitSequenceFileJob 运行结束");

        /**
         * 根据生成的训练集sequence file，统计各类别文档各有多少个
         */
        System.out.println("开始统计各类别文档数目");
        TypeNumCountJob typeNumCountJob = new TypeNumCountJob();
        ToolRunner.run(conf, typeNumCountJob, args);
        System.out.println("TypeNumCountJob 运行结束");

        /**
         * 根据生成的训练集sequence file，统计每个文档类型中各单词的出现次数
         */
        System.out.println("开始统计每个文档类型中各单词的出现次数");
        WordInTypeCountJob wordInTypeCountJob = new WordInTypeCountJob();
        ToolRunner.run(conf, wordInTypeCountJob, args);
        System.out.println("WordInTypeCountJob 运行结束");

        /**
         * 根据生成的训练集sequence file，统计每个类型文档内的单词总数
         */
        System.out.println("开始统计每个类型文档内的单词总数");
        WordSumInTypeCountJob wordSumInTypeCountJob = new WordSumInTypeCountJob();
        ToolRunner.run(conf, wordSumInTypeCountJob, args);
        System.out.println("WordSumInTypeCountJob 运行结束");

        /**
         * 开始处理测试集的数据，生成一个 sequence file
         */
        System.out.println("开始处理测试集的数据，生成一个 sequence file");
        conf = new Configuration();
        conf.set("INPUT_PATH", Const.GET_TEST_DATA_INPUT_PATH);
        conf.set("OUTPUT_PATH", Const.GET_TEST_DATA_SEQUENCE_FILE_PATH);
        conf.set("TYPE_LIST", Const.TYPE_LIST);

        initSequenceFileJob = new InitSequenceFileJob();
        ToolRunner.run(conf, initSequenceFileJob , args);
        System.out.println("测试及文件 InitSequenceFileJob 运行结束");

        /**
         * 读取测试集数据并计算训练集的先验概率与条件概率
         * 计算测试机每个文档的分类概率
         */
        System.out.println("开始计算训练集的先验概率与条件概率 + 计算测试机每个文档的分类概率");
        NaiveBayesClassificationJob naiveBayesClassificationJob = new NaiveBayesClassificationJob();
        ToolRunner.run(conf, naiveBayesClassificationJob, args);
        System.out.println("NaiveBayesClassificationJob 运行结束");

        /**
         * 进行预测后的评估工作，计算各文档FP、TP、FN、TN、Precision、Recall、F1以及整体的宏平均、微平均。
         */
        System.out.println("开始进行评估工作");
        EvaluationJob evaluationJob = new EvaluationJob();
        ToolRunner.run(conf, evaluationJob, args);
        System.out.println("EvaluationJob 运行结束");
    }
}
