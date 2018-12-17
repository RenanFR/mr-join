package hadoop.join.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable chaveLinha, Text linha, Mapper<LongWritable, Text, Text, Text>.Context contexto)
			throws IOException, InterruptedException {
		
		String[] conteudoLinha = linha.toString().split(",");
		contexto.write(new Text(conteudoLinha[0]), new Text("LOCATION," + conteudoLinha[1]));
		
	}
	
}
