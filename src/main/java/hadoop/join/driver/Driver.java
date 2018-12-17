package hadoop.join.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.join.mapper.LocationMapper;
import hadoop.join.mapper.PeopleMapper;
import hadoop.join.reducer.PeopleLocationReducer;

public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputDataset = new Path("file:////home/rodrir23/data-test/locais.txt/");
		Path inputDataset1 = new Path("file:////home/rodrir23/data-test/pessoas.txt/");
		Path outputDir = new Path("file:////home/rodrir23/data-test/output/");
		
		Configuration configuration = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "JOIN MAP REDUCE");
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(PeopleMapper.class);
		job.setMapperClass(LocationMapper.class);
		job.setReducerClass(PeopleLocationReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, inputDataset1, TextInputFormat.class, PeopleMapper.class);
		MultipleInputs.addInputPath(job, inputDataset, TextInputFormat.class, LocationMapper.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		outputDir.getFileSystem(job.getConfiguration()).delete(outputDir, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
	
}
