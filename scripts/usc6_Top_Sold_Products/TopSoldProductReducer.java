import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class TopSoldProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		// computes the number of occurrences of a single word
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		// puts the number of occurrences of this word into the map.
		// We need to create another Text object because the Text instance
		// we receive is the same for all the words
		countMap.put(new Text(key), new IntWritable(sum));
	}


	/**
	 * The combiner retrieves every word and puts it into a Map: if the word already exists in the
	 * map, increments its value, otherwise sets it to 1.
	 */

	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		//LinkedHashMap will keep the keys in the order they are inserted
		//which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		Map<Text, IntWritable> sortedMap = sortByValues(countMap);

		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == 10) {
				break;
			}
			context.write(key, sortedMap.get(key));
		}
	}
}
