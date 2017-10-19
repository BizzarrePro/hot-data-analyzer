package priv.geekliu.graduation.classifier;

import java.io.File;
import java.util.List;
import priv.geekliu.graduation.struct.Data;
import priv.geekliu.graduation.struct.PartitionTable;


public abstract class SerialForwardTemplate extends AbstractClassifier{
	protected PartitionTable<Long, Data> globalMap = null; 
	public abstract void read(File fp);
	public abstract List<Data> scanHotData();
	public SerialForwardTemplate(PartitionTimeStrategy partStategy) {
		super(partStategy);
		globalMap = new PartitionTable<>(1000);
	}
	
	@Override
	public List<Data> classify(File fp, ReadMode mode){
		return classify(fp);
	}
	
	@Override
	public List<Data> classify(File fp){
		List<Data> hotData = null;
		LOGGER.setLevel(java.util.logging.Level.INFO);
		LOGGER.info("The Forward algorithm begins execution," +
				" current memory occupied is " + ((double)getMemoryState() / 1000000.0) + "MB");
		long curr = System.currentTimeMillis();
		read(fp);
		LOGGER.info("Read file has ended, the total time-consuming is " + 
				((double)getTimeConsuming(curr) / 1000.0) + " seconds");
		hotData = scanHotData();
		LOGGER.info("The total time taken for the algorithm is " + 
				((double)getTimeConsuming(curr) / 1000.0) + " seconds");
		
		return hotData;
	}
}
