# Notes

```java
public class SentimentAnalysis {

    // 这里面包含两个 public static class, 分别继承了 Mapper 和 Reducer
    //--------------------------------------------------------------------------------------------
    // 我们自己的 Mapper 类
    //--------------------------------------------------------------------------------------------
    // 我们自己继承 Mapper 类的时候基本框架是要有这个 Map<,> 的参数，并且 Override map().

    // 先来看我们继承的这个 Mapper<Object, Text, Text, IntWritable>:
    // 这里的 Mapper 是一个预先定义好的类，那么后面的参数代表什么呢。

    // 在 mapreduce 中，我们所有的数据流动都是以 key-value pair 的形式来进行流动的，
    // 所以，所有数据的输入和输出都是以 key-value pair 的形式出现的

    // 这里 Mapper 的四个参数中，前两个表示的是当前 Mapper 的 input, 后两个是 output:
    // 1. Object:      input key-value pair 中的 key, 表示现在读入的这一行在文件中的 offset 是在哪里(不用管)
    // 2. Text:        input key-value pair 中的 value, 就是读入的文件中的每一行 (默认按行来读),
    //                 可以理解为一个用 Writable Class 包裹着的 String (后面会详细讲)
    // 3. Text:        输出的 key, 这里输出的 key-value pair 是可以自己定义的，
    //                 我们想要输出当前的情感是 positive/neutral/negative 以及各自出现了多少次
    //                 所以就是要输出 <String, Integer>。
    // 4. IntWritable: 理解为 int, 即前面 Text 代表的情感出现了多少次
    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {

        public Map<String, String> emotionDic = new HashMap<String, String>();

        @Override
        // 由于 Emotion Dictionary 只需要创建一次，所以这个创建过程不能写在后面的 map() 函数中
        // 所以我们需要一个初始化的函数，来进行一些初始化的工作。它只会在 initialize Mapper 时调用一次
        // 这里传入的 Context class 是 map-reduce 与 map-reduce 之外的环境交流的媒介。
        // 这里的 "媒介" 是什么意思呢？map-reduce 运行的时候，它并不知道和谁来交流
        // 比如要读文件的时候要和 HDFS 来交流，想写入数据库的时候要和 MySQL 交流。这就需要这个 context.
        public void setup(Context context) throws IOException{
            Configuration configuration = context.getConfiguration();
            // 我们不想在代码里写死这个 dict file 的路径，想要从命令行传入。
            // 但是 Mapper 没法直接读取命令行的数据。所以只能在 main 函数中读取命令行的输入。
            // 那么 main 函数怎么把读取的路径传递给我们的 Mapper 呢，就需要 configuration.set():
            // 后面 main 函数中有: configuration.set("dictionary", args[2])
            // 所以我们这里在 configuration 里面设置一个 "dictionary" 作为 key
            String dicName = configuration.get("dictionary", "");

            // 把 dict 文件读进来
            BufferedReader br = new BufferedReader(new FileReader(dicName));
            String line = br.readLine();

            while (line != null) {
                String[] word_feeling = line.split("\t");
                emotionDic.put(word_feeling[0].toLowerCase(), word_feeling[1]);
                line = br.readLine();
            }
            br.close();
        }

        @Override
        // 这个函数的任务:
        // Read file from hdfs:  我们继承 Mapper 的时候已经定义了第二个参数是从文件中读入的一行文件。
        // Split into words:     把这一行话根据空格拆分
        // Lookup in emotionDic: 从情感库中查询，把每个 word 转化为一种情感
        // Write to Reducer:     把结果写出到 Reducer 中
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString().trim();
            // 根据空格进行拆分，正则表达式表示可以按照多个空格或者 tab 进行拆分
            String[] words = line.split("\\s+");
            // 我们的 emotion dict 是一个本地的文件，想要查询每个单词就要建一个 HashMap emotionDic.
            // 这个 HashMap 只需要被创建一次，不用被更新.
            for (String word: words) {
                if (emotionDic.containsKey(word.trim().toLowerCase())) {
                    // 用 context.write 来写出
                    // Mapper 生成的是一个中间值，它不会直接写到 HDFS 上，会写到当前机器的硬盘上
                    // 注意我们的输入仍然是 key-value pair
                    // 由于我们每次都只处理一个单词，所以这个出现次数是 1
                    context.write(
                        new Text(fileName + "\t" + emotionDic.get(word.toLowerCase())),
                        new IntWritable(1)
                    );
                }
            }

        }
    }

    //--------------------------------------------------------------------------------------------
    // Reducer 类
    //--------------------------------------------------------------------------------------------
    // Reducer 会把所有相同的 key 所对应的 value 收集到一起去。
    // 基本框架是要重写 reduce 函数
    // 注意这个 Reducer Class 的输入的前两个 arguments 必须是上面的 Mapper 的输出的 key-value pair
    // 后两个参数是我们自己根据需求定义的 key-value pair
    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        // 注意这里的第二个参数变成了 Iterable<IntWritable>, 可以理解为一个 List
        // 因为从 Mapper 收集到的对于每个 key (比如 "positive")的结果都会通过 shuffle 过程来 transport 和 sort.
        // 假设有三个单词对应 "positive", 那么它对应的所有的 value 就是一个包含三个 "1" 的 List
        // shuffle 是 Reducer 中的一个步骤，它先 sort 所有的数据，再把相同 key 对应的所有 value 都能被收集到一台机器去。
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("dictionary", args[2]); // 设置 emotion dict file 的路径

        // 当前 job 的其它设置
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
```
