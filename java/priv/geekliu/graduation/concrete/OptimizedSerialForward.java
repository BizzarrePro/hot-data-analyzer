package priv.geekliu.graduation.concrete;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.classifier.PartitionTimeStrategy;
import priv.geekliu.graduation.classifier.SerialForwardTemplate;
import priv.geekliu.graduation.struct.Data;
import priv.geekliu.graduation.struct.ForwRecStats;

public class OptimizedSerialForward extends SerialForwardTemplate{
	private static final long BLOCK_SPACE_SIZE = 16l;
	private int timeSlice = 1;
	private long recordNum = 0;
	public OptimizedSerialForward(PartitionTimeStrategy partition) {
		super(partition);
	}

	@Override
	public void read(File fp) {
		
		//for logging
		int gap = partition.getPartitionNum() / 5;
		long maxMemoryOccupied = 0l;
		//for logging
		long addrSnapchat = -BLOCK_SPACE_SIZE - 1;
		double estSnapchat = 0.0;
		try (BufferedReader br = new BufferedReader(new FileReader(fp))){
			String record = null;
			while((record = br.readLine()) != null){
				String[] trace = record.split(" ");
				if(trace.length != 9)
					continue;
				long blockAddress = Long.parseLong(trace[3]);
				Data value = null;
				if((value = globalMap.get(blockAddress)) == null)
					globalMap.put(blockAddress, new ForwRecStats(timeSlice, blockAddress));
				else {
					ForwRecStats status = (ForwRecStats) value;
					if(status.getCurrTimeSlice() != timeSlice) {
						status.updatePreviousTimeSlice();
						status.setCurrTimeSlice(timeSlice);
						if(Math.abs(blockAddress - addrSnapchat) <= BLOCK_SPACE_SIZE)
							status.setFrequencyEstimate(estSnapchat);
						else
							status.updateFrequencyByExponentialSmoothing(true);
						addrSnapchat = blockAddress;
						estSnapchat = status.getFrequencyEstimate();
					}
				}
				if(recordNum % ((PartitionByAccessNum) partition).getPartitionSize(fp) == 0) {
					//Report state for each period of time
					if(timeSlice % gap == 0) {
						long memoryState = getMemoryState();
						if(memoryState > maxMemoryOccupied)
							maxMemoryOccupied = memoryState;
						LOGGER.info("Forward algorithm has proccessed " + timeSlice + 
								" time slices, the current memory consumption is " + 
								((double)memoryState / 1000000.0) + "MB");
					}
					timeSlice++;
				}
				recordNum++;
			}
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		LOGGER.info("Memory maximum consumption is " +
				((double)maxMemoryOccupied / 1000000.0) + "MB in read stage");
		LOGGER.info("The total record number is " + recordNum + 
				", hot data size is " + (recordNum * HOT_DATA_PERCENT));
	}

	@Override
	public List<Data> scanHotData() {
		PriorityQueue<Data> maxHeap = new PriorityQueue<>(1000);
		long size = (long)(recordNum * HOT_DATA_PERCENT);
		Iterator<Entry<Long, Data>> iter = globalMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Long, Data> entry = iter.next();
			maxHeap.offer(entry.getValue());
		}
		for(int i = 0; !maxHeap.isEmpty() && i < size; i++)
			globalMap.putInto(maxHeap.poll());
		return globalMap.getHotSet();
	}

}
