package priv.geekliu.graduation.concrete;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.ConsoleHandler;

import priv.geekliu.graduation.classifier.ParallelForwardTemplate;
import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.classifier.PartitionByTimeStamp;
import priv.geekliu.graduation.classifier.PartitionTimeStrategy;
import priv.geekliu.graduation.struct.Data;
import priv.geekliu.graduation.struct.ForwRecStats;
import priv.geekliu.graduation.struct.PartitionTable;

public class ParallelForwardPartByTimeStmp extends ParallelForwardTemplate {
	private long timeSliceInterval = 0;
	private int timeSliceNumInSinglePartition = 0;
	private static ExecutorService service = Executors.newFixedThreadPool(THREADPOOL_SIZE);
	private static PartitionTable<Long, Data> globalSortedMap = new PartitionTable<>(1000);
	private static PartitionTable<Long, Data>[] partTable;
	public ParallelForwardPartByTimeStmp(PartitionTimeStrategy partStategy,
			int threadNum) {
		super(partStategy, threadNum);
		//ConsoleHandler handler = new ConsoleHandler();
		//LOGGER.addHandler(handler);
		LOGGER.setLevel(java.util.logging.Level.INFO);
	}

	@Override
	public void partition(File fp, File[] part) {
		long currTimeStamp, firstTimeStamp, lastTimeStamp;
		int timeSliceIndex, partitionIndex;
		
		LOGGER.info("Partition by time slice start.");
		
		PartitionByTimeStamp timeStampStatus = ((PartitionByTimeStamp) partition);
		timeSliceInterval = timeStampStatus.getPartitionSize(fp);
		firstTimeStamp = timeStampStatus.getStartTimeStamp();
		lastTimeStamp = timeStampStatus.getLastTimeStamp();
		timeSliceNumInSinglePartition = timeStampStatus.getPartitionNum() / threadNum;
		BufferedWriter[] writers = initWriters(part, threadNum);
		try (BufferedReader br = new BufferedReader(new FileReader(fp))){
			String record = null;
			while((record = br.readLine()) != null){
				String[] blockTraces = record.split(" ");
				if(blockTraces.length != 9)
					continue;
				currTimeStamp = Long.parseLong(blockTraces[0]);
				timeSliceIndex = (int)((currTimeStamp - firstTimeStamp) / timeSliceInterval) + 1;
				//TODO debug index of time slice, time slice may be zero here
				partitionIndex = timeSliceIndex / timeSliceNumInSinglePartition;
				if(partitionIndex >= threadNum) {
					partitionIndex = threadNum - 1;
					timeSliceIndex = timeStampStatus.getPartitionNum();
				}
				writers[partitionIndex].append(record + " " + timeSliceIndex + "\r\n");
			}
			LOGGER.info("Partitioning by time slice has finished, last time stamp is " + lastTimeStamp + 
					". time slice interval is " + timeSliceInterval);
		} catch (FileNotFoundException fe){
			fe.printStackTrace();
		} catch (IOException ioe){
			ioe.printStackTrace();
		} finally {
			for(BufferedWriter bw : writers){
				try {
					bw.flush();
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

	@Override
	public ArrayList<ArrayList<Map.Entry<Long, Data>>> read(File fp) {
		//TODO check capacity of list
		ArrayList<ArrayList<Map.Entry<Long, Data>>> partList = new ArrayList<>(threadNum);
		for(int i = 0; i < threadNum; i++)
			partList.add(new ArrayList<Map.Entry<Long, Data>>(1000));
		long currTimeStamp, firstTimeStamp, lastTimeStamp, blockAddress;
		int timeSliceIndex, partitionIndex;
		PartitionByTimeStamp timeStampStatus = ((PartitionByTimeStamp) partition);
		timeSliceInterval = timeStampStatus.getPartitionSize(fp);
		firstTimeStamp = timeStampStatus.getStartTimeStamp();
		lastTimeStamp = timeStampStatus.getLastTimeStamp();
		timeSliceNumInSinglePartition = timeStampStatus.getPartitionNum() / threadNum;
		
		try (BufferedReader br = new BufferedReader(new FileReader(fp))){
			String record = null;
			while((record = br.readLine()) != null){
				String[] blockTraces = record.split(" ");
				if(blockTraces.length != 9)
					continue;
				currTimeStamp = Long.parseLong(blockTraces[0]);
				blockAddress = Long.parseLong(blockTraces[3]);
				timeSliceIndex = (int)((currTimeStamp - firstTimeStamp) / timeSliceInterval);
				//TODO debug index of time slice, here time slice may be zero
				partitionIndex = timeSliceIndex / timeSliceNumInSinglePartition;
				if(partitionIndex >= threadNum) {
					partitionIndex = threadNum - 1;
					timeSliceIndex = timeStampStatus.getPartitionNum();
				}
				
				partList.get(partitionIndex).add(new AbstractMap.SimpleImmutableEntry<Long, Data>(blockAddress,
						new ForwRecStats(timeSliceIndex + 1, blockAddress)));
				
			}
			LOGGER.info("Partitioning by time slice has finished, last time stamp is " + lastTimeStamp + 
					". time slice interval is " + timeSliceInterval);
		} catch (FileNotFoundException fe){
			fe.printStackTrace();
		} catch (IOException ioe){
			ioe.printStackTrace();
		}
		return partList;
	}
	
	@SuppressWarnings("unchecked")
	public void dispatch(ArrayList<ArrayList<Map.Entry<Long, Data>>> partList) {
		Collector[] collectors = new Collector[threadNum];
		partTable = (PartitionTable<Long, Data>[]) new PartitionTable<?, ?>[threadNum];
		ArrayList<Future<PartitionTable<Long, Data>>> futureList = new ArrayList<>();
		for(int i = 0; i < threadNum; i++) {
			collectors[i] = new Collector(partList.get(i), i + 1);
			futureList.add(service.submit(collectors[i]));
		}
		
		for(int i = 0; i < threadNum; i++){
			try {
				partTable[i] = futureList.get(i).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		service.shutdown();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void dispatch(File[] part) {
		Collector[] collectors = new Collector[threadNum];
		partTable = (PartitionTable<Long, Data>[]) new PartitionTable<?, ?>[threadNum];
		ArrayList<Future<PartitionTable<Long, Data>>> futureList = new ArrayList<>();
		for(int i = 0; i < threadNum; i++) {
			collectors[i] = new Collector(part[i], i + 1);
			futureList.add(service.submit(collectors[i]));
		}

		for(int i = 0; i < threadNum; i++){
			try {
				partTable[i] = futureList.get(i).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		service.shutdown();
	}

	@Override
	public List<Data> merge() {
		long prev = System.currentTimeMillis();
		LOGGER.info("Integrate partition result starts.");
		
		smoothFrequency(partTable, threadNum - 1);
		
		LOGGER.info("Integrate partition result has finished, consumes " +
				((double)getTimeConsuming(prev) / 1000.0) + " seconds");
		return searchHotData();
	}
	public List<Data> searchHotData() {
		PriorityQueue<Data> maxHeap = new PriorityQueue<>(1000);
		Iterator<Entry<Long, Data>> iter = globalSortedMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Long, Data> entry = iter.next();
			maxHeap.offer(entry.getValue());
		}
		for(int i = 0; !maxHeap.isEmpty() && i < 100; i++)
			globalSortedMap.putInto(maxHeap.poll());
		return globalSortedMap.getHotSet();
	}
	private class Collector implements Callable<PartitionTable<Long, Data>> {
		private final int partIndex;
		private final File part;
		private final ArrayList<Map.Entry<Long, Data>> partList;
		//TODO check map capacity
		private final PartitionTable<Long, Data> table = new PartitionTable<>(1000);
		/*The last time slice of the current partition is obtained 
			using the previous time slice of the next partition.
		*/
		public Collector(File part, int partIndex){
			this.part = part;
			this.partList = null;
			this.partIndex = partIndex;
		}
		public Collector(ArrayList<Map.Entry<Long, Data>> partList, int partIndex) {
			this.partList = partList;
			this.part = null;
			this.partIndex = partIndex;
		}
		@Override
		public PartitionTable<Long, Data> call() throws Exception {
			//test
			long prev = System.currentTimeMillis();
			LOGGER.info("Collector " + Thread.currentThread().getName() + 
					" starts collect hot data in each partition.");
			
			if(part == null)
				scanList();
			else
				scanFile();
			
			LOGGER.info("Collector " + Thread.currentThread().getName() + 
					" has collected, consumes " + 
					((double)getTimeConsuming(prev) / 1000.0) + " seconds" );
			return table;
		}
		
		private void scanList() {
			Iterator<Map.Entry<Long, Data>> iter = partList.iterator();
			while(iter.hasNext()) {
				Map.Entry<Long, Data> entry = iter.next();
				ForwRecStats value = null;
				if((value = (ForwRecStats) table.get(entry.getKey())) == null)
					table.put(entry.getKey(), entry.getValue());
				else 
					value.accumulatePartEstimate(((ForwRecStats) entry.getValue()).getCurrTimeSlice());
			}
		}
		
		private void scanFile() {
			long factor = timeSliceNumInSinglePartition;
			int prevSlice = 0;
			try (BufferedReader br = new BufferedReader(new FileReader(part))){
				for(String record = br.readLine();
						record != null;
						record = br.readLine()){
					String[] trace = record.split(" ");
					long blockAddress = Long.parseLong(trace[3]);
					int timeSlice = Integer.parseInt(trace[9]) + 1;
					ForwRecStats value = null;
					if((value = (ForwRecStats) table.get(blockAddress)) == null)
						table.put(blockAddress, new ForwRecStats(timeSlice, blockAddress));
					else if (value.getCurrTimeSlice() != timeSlice) {
						value.setCurrTimeSlice(timeSlice);
						value.accumulatePartEstimate(factor);
					}

					if(prevSlice != timeSlice && factor >= 0)
						factor --;
					prevSlice = timeSlice;
				}
			} catch (FileNotFoundException fe){
				fe.printStackTrace();
			} catch (IOException ioe){
				ioe.printStackTrace();
			}
		}
	}
	/*
	 * Use a recursion to solve the serial portion of exponential smoothing 
	   expansions with a minimum amount of code, but there is a risk of stack overflow.
	*/
	private PartitionTable<Long, Data> smoothFrequency(PartitionTable<Long, Data>[] partTable, int index){
		if(index < 0)
			return null;
		PartitionTable<Long, Data> prevTable = smoothFrequency(partTable, index - 1);
		Iterator<Map.Entry<Long, Data>> iter = partTable[index].entrySet().iterator();
		while(iter.hasNext()){
			Data entry = iter.next().getValue();
			//The third parameter of this function call is not the same as given in the paper, 1 is given in paper.
			
			if(prevTable == null)
				((ForwRecStats) entry).obtainOverallFrequency( 
						timeSliceNumInSinglePartition - 1, 0);
			else {
				Data prevEntry = globalSortedMap.get(entry.getBlockAddress());
				((ForwRecStats) entry).obtainOverallFrequency(
							timeSliceNumInSinglePartition - 1, 
							prevEntry == null ? 0.0 : prevEntry.getFrequencyEstimate());
		
			}
			globalSortedMap.put(entry.getBlockAddress(), entry);
		}
		return partTable[index];
	}
}
