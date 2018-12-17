package hadoop.join.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PeopleLocationReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text chaveRegistro, Iterable<Text> pessoasLocais, Reducer<Text, Text, Text, Text>.Context contexto)
			throws IOException, InterruptedException {
		
		List<String> pessoas = new ArrayList<String>();
		List<String> locais = new ArrayList<String>();
		
		Iterator<Text> registros = pessoasLocais.iterator();
		
		while (registros.hasNext()) {
			Text registroAtual = registros.next();
			String[] novoRegistro = registroAtual.toString().split(",");
			if (novoRegistro[0].equalsIgnoreCase("LOCATION")) {
				locais.add(novoRegistro[1].toString());
			} else {
				pessoas.add(novoRegistro[1].toString());
			}
		}
		
		if (!pessoas.isEmpty() && !locais.isEmpty()) {
			for (String pessoa : pessoas) {
				for (String local : locais) {
					contexto.write(chaveRegistro, new Text(pessoa + "," + local));
				}
			}
		}
		
	}
	
}
