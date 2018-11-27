import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TopSoldProductDriver 
{
	public static void main(String[] args) throws Exception 
	{
		Configuration conf=new Configuration();
		
		@SuppressWarnings("deprecation")
		Job job = new Job (conf, "Top Sold Products");
		
		job.setJarByClass(TopSoldProductDriver.class);
		job.setMapperClass(TopSoldProductMapper.class);
		job.setReducerClass(TopSoldProductReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(TopSoldProductPartitioner.class);
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1); 

	}
}  