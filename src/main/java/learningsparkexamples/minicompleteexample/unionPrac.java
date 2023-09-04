package learningsparkexamples.minicompleteexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class unionPrac {
	public static void main(String[] args) {
		//JavaのSpark Contextの生成
		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String localFilePath = "file:///C:\\spark\\README.md";
		//入力データのロード
		JavaRDD<String> lines = sc.textFile(localFilePath);
		
		//filter()変換の呼び出し
		JavaRDD<String> pythonLines = lines.filter(new Function<String, Boolean>() {
			@Override
			//linesの各行がlineとして一度ずつ呼ばれる
			public Boolean call(String line) throws Exception {
				//Pythonという文字列があるときのみtrueを返す
				return line.contains("Python");
			}
		});
		
		//filter()変換の呼び出し
		JavaRDD<String> javaLines = lines.filter(new Function<String, Boolean>() {
			@Override
			//linesの各行がlineとして一度ずつ呼ばれる
			public Boolean call(String line) throws Exception {
				//Pythonという文字列があるときのみtrueを返す
				return line.contains("Java");
			}
		});
		
		//Pythonもしくはjavaを含む行を取得
		JavaRDD<String> javaAndPythonLines = pythonLines.union(javaLines);

		
		// Print the filtered lines (optional)
        for (String line : javaAndPythonLines.collect()) {
            System.out.println("結果"+line);
        }

        // Close the JavaSparkContext
        sc.close();
	}	
}
