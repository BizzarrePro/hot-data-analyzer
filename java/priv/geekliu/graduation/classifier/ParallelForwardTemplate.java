package priv.geekliu.graduation.classifier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import priv.geekliu.graduation.struct.Data;

public abstract class ParallelForwardTemplate extends AbstractClassifier{
	protected int threadNum = 1;
	protected static final int THREADPOOL_SIZE = 10;
	
	public abstract void partition(File fp, File[] part);
	public abstract ArrayList<ArrayList<Map.Entry<Long, Data>>> read(File fp);
	public abstract void dispatch(File[] part);
	public abstract void dispatch(ArrayList<ArrayList<Map.Entry<Long, Data>>> part);
	public abstract List<Data> merge();
	
	public ParallelForwardTemplate(PartitionTimeStrategy partStategy, int threadNum) {
		super(partStategy);
		this.threadNum = threadNum;
	}
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
	
	@Override
	public List<Data> classify(File fp, ReadMode mode){
		List<Data> hotData = null;
		try {
			switch(mode) {
			case READ_FROM_PARTITION_INDIRECTLY:
				File[] part = new File[threadNum];
				
				long prev = System.currentTimeMillis();
				long total = System.currentTimeMillis();
				
				splitTrace(part, threadNum);
				LOGGER.info("Split trace consumes " + 
						((double)getTimeConsuming(prev) / 1000.0) + " seconds" );
				
				prev = System.currentTimeMillis();
				partition(fp, part);
				LOGGER.info("Partition consumes " + 
						((double)getTimeConsuming(prev) / 1000.0) + " seconds" );
				
				prev = System.currentTimeMillis();
				dispatch(part);
				LOGGER.info("Thread process partition consumes " + 
						((double)getTimeConsuming(prev) / 1000.0) + " seconds" );
				
				prev = System.currentTimeMillis();
				hotData = merge();
				LOGGER.info("Merge results consumes " + 
						((double)getTimeConsuming(prev) / 1000.0) + " seconds" );
				
				startShutDownHook();
				
				LOGGER.info("The execution of the PF algorithm consumes " + 
						((double)getTimeConsuming(total) / 1000.0) + " seconds" );
				
				break;
			case READ_IN_MEMORY_DIRECTLY:
				long prevTime = System.currentTimeMillis();
				ArrayList<ArrayList<Map.Entry<Long, Data>>> partRecs = read(fp);
				dispatch(partRecs);
				hotData = merge();
				LOGGER.info("The execution of the PF algorithm consumes " + 
						((double)getTimeConsuming(prevTime) / 1000.0) + " seconds" );
			}
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return hotData;
	}
	@Override
	public List<Data> classify(File fp){
		long fileSize = fp.length();
		long freeMemorySize = Runtime.getRuntime().freeMemory();
		List<Data> hotData = null;
		try {
			if(fileSize > 0.1 * freeMemorySize){
				File[] part = new File[threadNum];
				splitTrace(part, threadNum);
				partition(fp, part);
				dispatch(part);
				hotData = merge();
				startShutDownHook();
			} else {
				ArrayList<ArrayList<Map.Entry<Long, Data>>> partRecs = read(fp);
				dispatch(partRecs);
				hotData = merge();
			}
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return hotData;	
	}
}
