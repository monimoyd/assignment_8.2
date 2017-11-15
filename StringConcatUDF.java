package stringudf;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class StringConcatUDF extends UDF {

	public Text evaluate(final String SEP, ArrayList<String> args) {
		if ( SEP == null || args == null) {
			return null;
		}
		
		StringBuilder sb = new StringBuilder();
		for (String arg: args) {
			sb.append(arg);
			sb.append(SEP);
			
		}
		
		// Removing extra Separator from the tail
		if (args.size() > 0) {
			sb.delete(sb.lastIndexOf(SEP), sb.length());
		}
		
		return new Text(sb.toString());
		
	}
	
	

}
