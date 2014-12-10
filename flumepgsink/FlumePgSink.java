package flumepgsink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class FlumePgSink extends AbstractSink implements Configurable {
 
	private String queueSize;
	private String resultFile;
	private PrintWriter out;
    	
	
	
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			Event event = ch.take();
			byte[] datas = event.getBody();
			String ds = new String(datas, "UTF-8");
			String[] dsarr = ds.split(" ");
			System.out.println(dsarr[0]);
			out.println(dsarr[0]);
			
			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
			txn.rollback();
			status = Status.BACKOFF;
			if (t instanceof Error) {
				throw (Error)t;
			}
		} finally {
			txn.close();
		}
		return status;
	}

	@Override
	public void configure(Context context) {
		String queueSize = context.getString("queueSize", "10000");
		String resultFile = context.getString("resultFile", "/tmp/flume/result1.txt");
		this.queueSize = queueSize;
		this.resultFile = resultFile;
		
		try {
		  FileWriter outFile = new FileWriter(this.resultFile);
		  PrintWriter out = new PrintWriter(outFile);
		  this.out = out;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
