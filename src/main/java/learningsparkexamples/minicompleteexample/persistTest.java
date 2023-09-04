package learningsparkexamples.minicompleteexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class persistTest {
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
			public Boolean call(String line) throws Exception {
				return line.contains("Python");
			}
		});
		
		// Print the filtered lines (optional)
        for (String line : pythonLines.collect()) {
            System.out.println("結果"+line);
        }

        // Close the JavaSparkContext
        sc.close();
	}	
}
