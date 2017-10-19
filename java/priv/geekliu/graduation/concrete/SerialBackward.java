package priv.geekliu.graduation.concrete;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.io.input.ReversedLinesFileReader;
import priv.geekliu.graduation.classifier.PartitionTimeStrategy;
import priv.geekliu.graduation.classifier.SerialBackwardTemplate;
import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.struct.BackRecStats;
import priv.geekliu.graduation.struct.Data;

public class SerialBackward extends SerialBackwardTemplate{
	private double acceptThresh = 0.0;
	private int endTimeSlice = partition.getPartitionNum() + 1;
	private long recordSize = 0;
	private double kthLower = Double.MAX_VALUE;
	private int prevTimeSlice = -1;
	private long recordNum = 0;
	private long numOfRecInTimeSlice = 0;
	private long hotDataNum = 0;
	public SerialBackward(PartitionTimeStrategy partStrategy) {
		super(partStrategy);
	}

	@Override
	public boolean initialize(File fp, ReversedLinesFileReader fr) {
		String record = null;
		int krecordCount = 0;
		int currTimeSlice = -1;
		numOfRecInTimeSlice = ((PartitionByAccessNum) partition).getPartitionSize(fp);
		recordSize = numOfRecInTimeSlice * endTimeSlice;
		hotDataNum = (int)(recordSize * HOT_DATA_PERCENT);
		try {
			while(krecordCount <= hotDataNum && (record = fr.readLine()) != null){
				String[] trace = record.split(" ");
				if(trace.length != 9)
					continue;
				recordNum ++;
				long blockAddress = Long.parseLong(trace[3]);
				currTimeSlice = (int)(Math.abs(recordSize - recordNum) / numOfRecInTimeSlice) + 1;
				
				if(globalMap.containsKey(blockAddress)) 
					continue;
				
				BackRecStats recStatus = new BackRecStats(blockAddress, currTimeSlice);
				recStatus.updateBoundingAccessFrequencyEstimate(currTimeSlice, endTimeSlice);
				recStatus.updateLowerEstimate(1, endTimeSlice);
				recStatus.updateUpperEstimate(currTimeSlice, endTimeSlice);
				
				if(recStatus.getLowerEstimateLimit() < kthLower)
					kthLower = recStatus.getLowerEstimateLimit();
					
				globalMap.put(blockAddress, recStatus);
				prevTimeSlice = currTimeSlice;
				krecordCount ++;
			}
			acceptThresh = endTimeSlice - Math.log(kthLower) / Math.log(1 - Data.getAlpha());
		} catch ( IOException ioe ) {
			ioe.printStackTrace();
		}
		return krecordCount < hotDataNum;
	}

	@Override
	public void read(File fp, ReversedLinesFileReader fr) {
		try {
			String record = null;
			int currTimeSlice = -1;
			while((record = fr.readLine()) != null) {
				String[] trace = record.split(" ");
				if(trace.length != 9)
					continue;
				recordNum++;
				long blockAddress = Long.parseLong(trace[3]);
				
				currTimeSlice = (int)(Math.abs(recordSize - recordNum) / numOfRecInTimeSlice) + 1;
				BackRecStats recStatus = (BackRecStats) globalMap.get(blockAddress);
				if(recStatus == null) {
					if(currTimeSlice <= acceptThresh)
						continue;
					else {	
						recStatus = new BackRecStats(blockAddress, currTimeSlice);
						recStatus.updateBoundingAccessFrequencyEstimate(currTimeSlice, endTimeSlice);
						recStatus.updateLowerEstimate(1, endTimeSlice);
						recStatus.updateUpperEstimate(currTimeSlice, endTimeSlice);
						
//						if(recStatus.getLowerEstimateLimit() < kthLower)
//							kthLower = recStatus.getLowerEstimateLimit();
						globalMap.put(blockAddress, recStatus);
					}
					//do backward need to deduplicate in same time slice?
				} else if (recStatus.getTimeSlice() != currTimeSlice) {
					recStatus.setTimeSlice(currTimeSlice);
					recStatus.updateBoundingAccessFrequencyEstimate(currTimeSlice, endTimeSlice);
				}
				
				if( prevTimeSlice != currTimeSlice ) {
					BackRecStats value = null;
					kthLower = Double.MAX_VALUE;
					PriorityQueue<BackRecStats> heap = new PriorityQueue<>();
					Iterator<Map.Entry<Long, Data>> iter = globalMap.entrySet().iterator();
					while(iter.hasNext()){
						 value = (BackRecStats) iter.next().getValue();
						 value.updateLowerEstimate(1, endTimeSlice);
						 value.updateUpperEstimate(currTimeSlice, endTimeSlice);
						 heap.add(value);
						 //TODO it's a bug here
//						 if( value.getLowerEstimateLimit() < kthLower )
//							 kthLower = value.getLowerEstimateLimit();
					}
					for(int i = 0; !heap.isEmpty() && i < hotDataNum - 1; i++)
						kthLower = heap.poll().getLowerEstimateLimit();
					
					iter = globalMap.entrySet().iterator();
					while(iter.hasNext()) {
						value = (BackRecStats) iter.next().getValue();
						if( value.getUpperEstimateLimit() <= kthLower )
							iter.remove();
					}
					LOGGER.info("Table size: " + globalMap.size() + "Absolute value of HD size from GD size: " + Math.abs(globalMap.size() - hotDataNum));
					if(globalMap.size() == hotDataNum)
						break;
					acceptThresh = endTimeSlice - Math.log(kthLower) / Math.log(1 - Data.getAlpha());
				}
				prevTimeSlice = currTimeSlice;
			}
			for(Map.Entry<Long, Data> entry : globalMap.entrySet())
				globalMap.putInto(entry.getValue());
			Collections.sort(globalMap.getHotSet(), new RecComparator());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
