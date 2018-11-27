import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopSoldProductPartitioner extends Partitioner<Text, IntWritable>{

	public int getPartition(Text key, IntWritable value, int numPartitions) 
	{
		String word = key.toString();
		
		if(word.equals("MILK")||word.equals("BREAD")||word.equals("CURD")||word.equals("CREAM")||word.equals("WHEAT")
				||word.equals("SOAP")||word.equals("OIL")||word.equals("BUN")||word.equals("COFFEE")||word.equals("BUTTER")
				||word.equals("BAGS")||word.equals("ELACHI")||word.equals("ORANGE")||word.equals("MATCHES")||word.equals("SALT")
				||word.equals("ATTA")||word.equals("CASHEWNUT")||word.equals("NOODLES")||word.equals("JAM")||word.equals("RICE")
				||word.equals("CHEWINGUM")||word.equals("DEODORANT")||word.equals("ALMOND")||word.equals("GHEE")||word.equals("TEA")
				)
			return 0;
		else
			return 1;		
	}
}