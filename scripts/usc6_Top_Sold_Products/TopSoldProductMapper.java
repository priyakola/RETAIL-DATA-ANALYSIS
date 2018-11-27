import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
public class TopSoldProductMapper extends Mapper<Object,
Text, Text, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);
	private Text item = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String str[]=value.toString().split(",");
		if(str[0].trim().equals("invD_icode"))
		{
			return;
		}
		else
		{
			item.set(str[7].trim());
			StringTokenizer itr = new StringTokenizer(item.toString());
			while (itr.hasMoreTokens()) 
			{
				item.set(itr.nextToken());
				context.write(item, one);
			}
		
		}
	}
}