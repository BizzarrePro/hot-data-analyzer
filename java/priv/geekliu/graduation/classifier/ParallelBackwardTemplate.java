package priv.geekliu.graduation.classifier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import org.apache.commons.io.input.ReversedLinesFileReader;

import priv.geekliu.graduation.struct.Data;

public abstract class ParallelBackwardTemplate extends AbstractClassifier {
	protected int threadNum = 1;
	protected static final int THREADPOOL_SIZE = 10;
	protected File[] part;
	public abstract void partition(File fp);
	public abstract void read(File fp);
	public abstract List<Data> process();
	
	protected static interface Communicatable {
		public abstract void initialize(ReversedLinesFileReader fr);
		public abstract void report();
		public abstract void tightenBounds(ReversedLinesFileReader fr);
		public abstract void shutDown();
	}
	
	protected static enum Command { Initialization, ReportCount, TightenBounds, Finallize}
	
	public static void startShutDownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				File[] garbageTempFiles = tempDir.listFiles();
				for(File f : garbageTempFiles)
					f.delete();
				tempDir.delete();
			}
		});
	}
	
	public ParallelBackwardTemplate(PartitionTimeStrategy partition, int threadNum) {
		super(partition);
		this.threadNum = threadNum;
		part = new File[threadNum];
		LOGGER.setLevel(Level.INFO);
	}

	@Override
	public List<Data> classify(File fp, ReadMode mode) {
		List<Data> hotData = null;
		long curr = System.currentTimeMillis();
		try {
			switch(mode) {
			case READ_FROM_PARTITION_INDIRECTLY:
				splitTrace(part, threadNum);
				LOGGER.info("Partition files have been created.");
				long prev = System.currentTimeMillis();
				partition(fp);
				LOGGER.info("Split trace consumes " + 
						((double)getTimeConsuming(prev) / 1000.0) + " seconds");
				for(File f : part)
					LOGGER.info("The size of " + f.getName() + " is " + f.length());
				hotData = process();
				startShutDownHook();
				break;
			case READ_IN_MEMORY_DIRECTLY:
				read(fp);
				hotData = process();
			}
			System.out.println(((double)getTimeConsuming(curr) / 1000.0) + " seconds");
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return hotData;
	}
	//TODO implement default read mode
	@Override
	public List<Data> classify(File fp) {
		List<Data> hotData = null;
		long curr = System.currentTimeMillis();
		try {
			splitTrace(part, threadNum);
			LOGGER.info("Partition files have been created.");
			long prev = System.currentTimeMillis();
			partition(fp);
			LOGGER.info("Split trace consumes " + 
					((double)getTimeConsuming(prev) / 1000.0) + " seconds");
			for(File f : part)
				LOGGER.info("The size of " + f.getName() + " is " + f.length());
			hotData = process();
			startShutDownHook();
			System.out.println(((double)getTimeConsuming(curr) / 1000.0) + " seconds");
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return hotData;
	}
	
	protected static class PartitionStatus {
		private double knth = 0.0;
		private int lowCount = 0;
		private int upCount = 0;
		public PartitionStatus(){}
		public PartitionStatus(int lowCount, int upCount) {
			this.lowCount = lowCount;
			this.upCount= upCount;
		}
		public PartitionStatus(double knth, int lowCount, int upCount) {
			this.knth = knth;
			this.lowCount = lowCount;
			this.upCount = upCount;
		}
		public double getKnth() {
			return knth;
		}
		public int getLowCount() {
			return lowCount;
		}	
		public int getUpCount() {
			return upCount;
		}
		public String toString(){
			return "Knth: " + knth + " lowCount: " + lowCount + " upCount: " + upCount;
		}
	}
}
