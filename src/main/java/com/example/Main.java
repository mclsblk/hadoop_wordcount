package com.example;

import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import java.util.*;
import java.io.*;
import java.net.URI;

public class Main {

    // 读取文件并拆分单词，将单词按情感储存词频
    public static class mapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        //实现map()函数
        public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {  
            //将字符串拆解成单词
            loadStopWords(context);
            String line = value.toString();
            String[] parts = parseCSVLine(line);

            String sentence = preprocessText(parts[0]);

            if(parts.length >= 2){
                StringTokenizer itr = new StringTokenizer(sentence);

                while (itr.hasNext()) {  
                    String token = itr.nextToken();
                    token = token.trim();
                    if(stopWords.contains(token) || token.isEmpty() || token.length() < 2) continue;
                    String OutputKey = parts[1] + "_" + token;     
                    word.set(OutputKey);
                    //将分解后的一个单词写入word
                    context.write(word, one);
                }
            }


        }


        private String[] parseCSVLine(String line) {
            List<String> result = new ArrayList<>();
            boolean inQuotes = false;
            StringBuilder field = new StringBuilder();
            
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                
                if (c == '"') {
                    inQuotes = !inQuotes;
                } else if (c == ',' && !inQuotes) {
                    result.add(field.toString());
                    field.setLength(0);
                } else {
                    field.append(c);
                }
            }
            result.add(field.toString());
            
            return result.toArray(new String[0]);
        }

        private String preprocessText(String text) {
            if (text == null || text.isEmpty()) {
                return "";
            }
            
            text = text.replaceAll("^\"|\"$", "");
            text = text.toLowerCase();
            text = text.replaceAll("[^a-zA-Z'\\s]", " ");
            text = text.replaceAll("\\d+", " ");
            text = text.replaceAll("\\s+", " ").trim();
            
            return text;
        }

        private void loadStopWords(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {

                Path stopWordsPath = new Path(cacheFiles[0].toString());
                FileSystem fs = FileSystem.get(context.getConfiguration());
            
                try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(stopWordsPath)))) {
                    String stopWord;
                    while ((stopWord = reader.readLine()) != null) {
                        stopWord = stopWord.trim().toLowerCase();
                        if (!stopWord.isEmpty()) {
                            stopWords.add(stopWord);
                        }
                    }
                }
            } else {
                System.err.println("No stop words file found in cache");
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        //实现reduce()函数
        public void reduce(Text key, Iterable<IntWritable> values, Context context )
            throws IOException, InterruptedException {
            int sum = 0;
            //遍历迭代器values，得到同一key的所有value
            for (IntWritable val : values) {  
                sum += val.get();      
            }
            result.set(sum);
            //产生输出对<key, value>
            context.write(key, result);
        }
    }

    public static class TopWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> multipleOutputs;
        private TreeMap<Integer, List<String>> positiveWords = new TreeMap<>(Collections.reverseOrder());
        private TreeMap<Integer, List<String>> negativeWords = new TreeMap<>(Collections.reverseOrder());
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("🎯 TopWordsReducer.setup() 被 Hadoop 自动调用");
            try {
                multipleOutputs = new MultipleOutputs<>(context);
                System.out.println("✅ MultipleOutputs 初始化成功");
            } catch (Exception e) {
                System.err.println("❌ MultipleOutputs 初始化失败: " + e.getMessage());
                throw e;
            }
        }
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            System.out.println("🎯 TopWordsReducer.reduce() 被调用，key: " + key);
            String keyStr = key.toString();
            int totalCount = 0;
            
            // 计算该单词的总出现次数
            for (IntWritable val : values) {
                totalCount += val.get();
            }
            
            // 解析情感和单词
            int underscoreIndex = keyStr.indexOf('_');
            if (underscoreIndex != -1) {
                String sentiment = keyStr.substring(0, underscoreIndex);
                String word = keyStr.substring(underscoreIndex + 1);
                
                // 根据情感分类存储
                if (sentiment.equals("1")) {
                    positiveWords.computeIfAbsent(totalCount, k -> new ArrayList<>()).add(word);
                } else if (sentiment.equals("-1")) {
                    negativeWords.computeIfAbsent(totalCount, k -> new ArrayList<>()).add(word);
                }
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                // 输出正面情感Top 100到单独文件
                outputTopWordsToFile("positive", positiveWords, 100);
                
                // 输出负面情感Top 100到单独文件
                outputTopWordsToFile("negative", negativeWords, 100);
            } finally {
                // 确保MultipleOutputs被关闭
                multipleOutputs.close();
            }
        }
        
        /**
         * 输出指定情感的前N个高频词到指定文件
         */
        private void outputTopWordsToFile(String outputName, 
                                        TreeMap<Integer, List<String>> wordMap, 
                                        int topN) 
                throws IOException, InterruptedException {
            
            int count = 0;
            for (Map.Entry<Integer, List<String>> entry : wordMap.entrySet()) {
                int frequency = entry.getKey();
                List<String> words = entry.getValue();
                
                // 对同频率的单词按字母顺序排序
                Collections.sort(words);
                
                for (String word : words) {
                    if (count < topN) {
                        // 使用MultipleOutputs输出到指定名称的文件
                        multipleOutputs.write(outputName, new Text(word), new IntWritable(frequency));
                        count++;
                    } else {
                        break;
                    }
                }
                if (count >= topN) break;
            }
        }
    }
        

    public static void main(String[] args) throws Exception{ 
        //为任务设定配置文件
        Configuration conf = new Configuration();
        //命令行参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3){ 
        System.err.println("Usage: wordcount <in> <out> <stop words>");
        System.exit(2);
        } 

        // 获取输出路径
        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(conf);
   
        // 检查输出目录是否存在，如果存在则删除
        if (fs.exists(outputPath)) {
            System.out.println("Output directory already exists, deleting it...");
            fs.delete(outputPath, true); // true 表示递归删除
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Main.class); 

        job.setMapperClass(mapper.class);                 
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TopWordsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new Path(otherArgs[2]).toUri());

        //设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 设置Reduce任务数量
        job.setNumReduceTasks(1);

        // 添加命名输出定义
        MultipleOutputs.addNamedOutput(job, "positive", 
            TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "negative", 
            TextOutputFormat.class, Text.class, IntWritable.class);

        System.out.println("Starting stock news analysis job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}