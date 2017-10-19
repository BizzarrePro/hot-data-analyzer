package priv.geekliu.graduation.classifier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;

import priv.geekliu.graduation.concrete.OptimizedSerialForward;
import priv.geekliu.graduation.concrete.ParallelBackward;
import priv.geekliu.graduation.concrete.ParallelForwardPartByRecId;
import priv.geekliu.graduation.concrete.ParallelForwardPartByTimeStmp;
import priv.geekliu.graduation.concrete.SerialBackward;
import priv.geekliu.graduation.concrete.SerialForward;
import priv.geekliu.graduation.struct.Data;
import priv.geekliu.graduation.struct.ForwRecStats;

//Template pattern
public abstract class AbstractClassifier {
	protected static final File tempDir = new File("J:\\data\\temp");
	public static final Logger LOGGER = Logger.getGlobal();
	protected static double HOT_DATA_PERCENT = 0.0005;
	private static AbstractClassifier INSTANCE = null;
	public abstract List<Data> classify(File fp, ReadMode mode);
	public abstract List<Data> classify(File fp);
	protected PartitionTimeStrategy partition = null;
	public enum ReadMode { READ_IN_MEMORY_DIRECTLY, READ_FROM_PARTITION_INDIRECTLY }
	protected AbstractClassifier(PartitionTimeStrategy partition) {
		this.partition = partition;
		//ConsoleHandler handler = new ConsoleHandler();
		//LOGGER.addHandler(handler);
	}
	public void setHotDataPercent(double persent) {
		AbstractClassifier.HOT_DATA_PERCENT = persent;
	}
	public enum SerialOption{
		SERIAL_FORWARD, SERIAL_BACKWARD, SERIAL_OPTIMIZED_FORWARD
	}
	public enum ParallelOption{
		PARALLEL_FORWARD_WITH_RECORD_ID_PARTITION,
		PARALLEL_FORWARD_WITH_TIME_SLICE_PARTITION,
		PARALLEL_BACKWARD
	}
	public static AbstractClassifier invokeClassifier(
			SerialOption op, int timeSliceNum) throws Exception{
		if(INSTANCE != null)
			return INSTANCE;
		if(timeSliceNum < 2)
			throw new Exception("The number of time slices is too small");
		switch(op) {
		case SERIAL_FORWARD: 
			INSTANCE = new SerialForward(
					new PartitionByAccessNum(timeSliceNum));
			break;
		case SERIAL_BACKWARD: 
			INSTANCE = new SerialBackward(
					new PartitionByAccessNum(timeSliceNum));
			break;
		case SERIAL_OPTIMIZED_FORWARD: 
			INSTANCE = new OptimizedSerialForward(
					new PartitionByAccessNum(timeSliceNum));
		}
		return INSTANCE;
	}
	public static AbstractClassifier invokeClassifier(
			ParallelOption op, int timeSliceNum, int threadNum) throws Exception {
		if(INSTANCE != null)
			return INSTANCE;
		if(timeSliceNum < 2)
			throw new Exception("The number of time slices is too small");
		if(threadNum < 1)
			throw new Exception("The number of threads must be greater than 0");
		switch(op) {
		case PARALLEL_FORWARD_WITH_RECORD_ID_PARTITION:
			INSTANCE = new ParallelForwardPartByRecId(
					new PartitionByTimeStamp(timeSliceNum), threadNum);
			break;
		case PARALLEL_FORWARD_WITH_TIME_SLICE_PARTITION:
			INSTANCE = new ParallelForwardPartByTimeStmp(
					new PartitionByTimeStamp(timeSliceNum), threadNum);
			break;
		case PARALLEL_BACKWARD:
			INSTANCE = new ParallelBackward(
					new PartitionByAccessNum(timeSliceNum), threadNum);
		}
		return INSTANCE;
	}
	//If the log file is too large, split it into multiple slices
	protected void splitTrace(File[] partition, int pieceNum) throws IOException{
		
		if(tempDir.exists())
			tempDir.delete();
		tempDir.mkdir();
		String attr = "attrib +H " + tempDir.getAbsolutePath();
		Runtime.getRuntime().exec(attr);
		String absolutePrefixPath = tempDir.getAbsolutePath();
		for(int i = 0; i < pieceNum; i++){
			partition[i] = new File(absolutePrefixPath + "\\partition"+i+".blktrace");
			partition[i].createNewFile();
		}
		
	}
	
	public static BufferedWriter[] initWriters(File[] partition, int writerNum){
		FileWriter[] writers = new FileWriter[writerNum];
		BufferedWriter[] bw = new BufferedWriter[writerNum];
		try {
			for (int i = 0; i < writerNum; i++){
				writers[i] = new FileWriter(partition[i]);
				bw[i] = new BufferedWriter(writers[i]);
			}
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bw;
	}
	
	protected long getMemoryState() {
		return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
	}
	
	protected long getTimeConsuming(long prev) {
		return System.currentTimeMillis() - prev;
	}
	
	protected class RecComparator implements Comparator<Data> {
	
		public RecComparator() {}

		@Override
		public int compare(Data o1, Data o2) {
			if(o1.getFrequencyEstimate() == o2.getFrequencyEstimate())
				return 0;
			return o1.getFrequencyEstimate() > o2.getFrequencyEstimate() ? 1 : -1;
		}
		
	}
}
