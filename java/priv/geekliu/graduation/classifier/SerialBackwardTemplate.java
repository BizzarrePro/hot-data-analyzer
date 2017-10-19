package priv.geekliu.graduation.classifier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.input.ReversedLinesFileReader;

import priv.geekliu.graduation.struct.Data;
import priv.geekliu.graduation.struct.PartitionTable;

public abstract class SerialBackwardTemplate extends AbstractClassifier{
	protected PartitionTable<Long, Data> globalMap = null; 
	public abstract boolean initialize(File fp, ReversedLinesFileReader fr);
	public abstract void read(File fp, ReversedLinesFileReader fr);
	public SerialBackwardTemplate(PartitionTimeStrategy partition) {
		super(partition);
		globalMap = new PartitionTable<>(1000);
	}
	@Override
	public List<Data> classify(File fp, ReadMode mode) {
		return classify(fp);
	}

	//implement partition file
	@Override
	public List<Data> classify(File fp) {
		LOGGER.setLevel(java.util.logging.Level.INFO);
		long prev = System.currentTimeMillis();
		long total = System.currentTimeMillis();
		try (ReversedLinesFileReader fr = new ReversedLinesFileReader(fp, Charset.forName("UTF-8"))) {
			
			LOGGER.info("Fill heat table start.");
			boolean hasEnded = initialize(fp, fr);
			LOGGER.info("It takes " + ((double)getTimeConsuming(prev) / 1000.0) + " seconds to fill heat table.");
			prev = System.currentTimeMillis();
			if(!hasEnded)
				read(fp, fr);
			LOGGER.info("Backward scan consumes " + 
					((double)getTimeConsuming(prev) / 1000.0) + " seconds");
			LOGGER.info("The total time taken for the algorithm is " + 
					((double)getTimeConsuming(total) / 1000.0) + " seconds");
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return globalMap.getHotSet();
	}

}
