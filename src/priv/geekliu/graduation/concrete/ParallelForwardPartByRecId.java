package priv.geekliu.graduation.concrete;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.ConsoleHandler;

import priv.geekliu.graduation.classifier.ParallelForwardTemplate;
import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.classifier.PartitionByTimeStamp;
import priv.geekliu.graduation.classifier.PartitionTimeStrategy;
import priv.geekliu.graduation.struct.Data;
import priv.geekliu.graduation.struct.ForwRecStats;
import priv.geekliu.graduation.struct.PartitionTable;

public class ParallelForwardPartByRecId extends ParallelForwardTemplate{
	private static Thread[] workers;
	private static long recordNum = 0;
	private static BlockingQueue<List<Data>> coordinator;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(THREADPOOL_SIZE);
	private static CountDownLatch endGate;
	private long timeSliceSize;
	private long firstTimestamp = 0;
	public ParallelForwardPartByRecId(PartitionTimeStrategy partStrategy, int threadNum) {
		super(partStrategy, threadNum);
		workers = new Thread[threadNum];
		coordinator = new ArrayBlockingQueue<>(threadNum);
		endGate=  new CountDownLatch(threadNum - 1);
		ConsoleHandler handler = new ConsoleHandler();
		//LOGGER.addHandler(handler);
		LOGGER.setLevel(java.util.logging.Level.INFO);
	}

	@Override
	public void partition(File fp, File[] part) {
		//test
		long prev = System.currentTimeMillis();
		timeSliceSize = ((PartitionByTimeStamp) partition).getPartitionSize(fp);
		
		LOGGER.info("Calculate the slice size consumes " + 
				((double)getTimeConsuming(prev) / 1000.0) + 
				" seconds and time slice size is " + timeSliceSize );
		
		BufferedWriter[] writers = initWriters(part, threadNum);
		boolean hasGotten = true;
		try (BufferedReader br = new BufferedReader(new FileReader(fp))){
			String record = null;
			while((record = br.readLine()) != null){
				String[] blockTraces = record.split(" ");
				if(blockTraces.length != 9)
					continue;	
				if(hasGotten) {
					firstTimestamp = Long.parseLong(blockTraces[0]);
					hasGotten = false;
				}
				long blockAddress = Long.parseLong(blockTraces[3]);
				int hashValue = hash(blockAddress);
				writers[hashValue].append(record + "\r\n");
				recordNum++;
			}
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
	//TODO implement part by record id
	@Override
	public ArrayList<ArrayList<Map.Entry<Long, Data>>> read(File fp) {
		//test
		long prev = System.currentTimeMillis();
		LOGGER.info("Read log in memory start.");
		timeSliceSize = ((PartitionByTimeStamp) partition).getPartitionSize(fp);
		//TODO initialize capacity of part list
		ArrayList<ArrayList<Map.Entry<Long, Data>>> partList = new ArrayList<>((int)(fp.length() / 75));
		for(int i = 0; i < threadNum; i++)
			partList.add(new ArrayList<Map.Entry<Long, Data>>());
		boolean hasGotten = true;
		
		try (BufferedReader br = new BufferedReader(new FileReader(fp))) {
			String record = null;
			while((record = br.readLine()) != null) {
				String[] blockTraces = record.split(" ");
				if(blockTraces.length != 9)
					continue;	
				if(hasGotten) {
					firstTimestamp = Long.parseLong(blockTraces[0]);
					hasGotten = false;
				}
				long blockAddress = Long.parseLong(blockTraces[3]);
				long timestamp = Long.parseLong(blockTraces[0]);
				int hashValue = hash(blockAddress);
				int timeSlice = (int)((timestamp - firstTimestamp) / timeSliceSize);
	
				partList.get(hashValue).add(new 
						AbstractMap.SimpleImmutableEntry<Long, Data>(blockAddress,
								new ForwRecStats(timeSlice, blockAddress)));
				//test
//				if(recordNum % 100000 == 0)
//					System.out.println(recordNum);
				recordNum++;
			}
		} catch (FileNotFoundException fe){
			fe.printStackTrace();
		} catch (IOException ioe){
			ioe.printStackTrace();
		}
		LOGGER.info("IO consumes " + 
				((double)getTimeConsuming(prev) / 1000.0) + 
				" seconds.");
		return partList;
	}
	//TODO implement read from memory
	public void dispatch(ArrayList<ArrayList<Map.Entry<Long, Data>>> part) {
		for(int i = 0; i < threadNum; i++){
			workers[i] = new Thread(new Worker(part.get(i)));
			threadPool.execute(workers[i]);
		}
	}
	@Override
	public void dispatch(File[] part) {
		for(int i = 0; i < threadNum; i++){
			workers[i] = new Thread(new Worker(part[i], timeSliceSize));
			threadPool.execute(workers[i]);
		}
		LOGGER.info("all of worker threads start computing estimate frequency of trace");
	}

	@Override
	public List<Data> merge() {
//		int totalMergeNum = (partitionNum * 2) - 1;
		for(int i = 0; i < threadNum - 1; i++){
			try {
				List<Data> oneSet = coordinator.take();
				List<Data> theOtherSet = coordinator.take();
				Zipper zip = new Zipper(oneSet, theOtherSet);
				Thread merge = new Thread(zip, "MergeThread" + i);
				merge.start();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOGGER.info("The main thread is waiting for all threads to finish");
		try {
			endGate.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		threadPool.shutdown();
		List<Data> hotSet = null;
		try {
			hotSet = coordinator.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		LOGGER.info("All of top n records of partition have been merge completely," +
				" and result will be returned");
		return hotSet;
	}
	private int hash(long blockAddress) {
		return Math.abs(String.valueOf(blockAddress).hashCode() % threadNum);
	}
	
	//merge top k from n files
		private class Zipper implements Runnable {
			public List<Data> oneHotSet;
			public List<Data> theOtherSet;
			public Zipper(List<Data> oneHotSet, List<Data> theOtherHotSet){
				this.oneHotSet = oneHotSet;
				this.theOtherSet = theOtherHotSet;
			}
			@Override
			public void run() {
				LOGGER.info("Zipper " + Thread.currentThread().getName() + " starts work," +
						" it's trying to take finished partition tuple from blocking queue to merge it ");
				int hotDataSize = getHotDataNum(1);
				final List<Data> topnSet = new ArrayList<>(hotDataSize);
				int indexOne = 0, indexTheOther = 0;
				for(int i = 0; indexOne < oneHotSet.size() && 
						indexTheOther < theOtherSet.size() && 
						i <= hotDataSize; i ++){
					Data one = oneHotSet.get(indexOne);
					Data theOther = theOtherSet.get(indexTheOther);
					if(one.compareTo(theOther) == -1){
						topnSet.add(one);
						indexOne ++;
					} else {
						topnSet.add(theOther);
						indexTheOther ++;
					}
				}
				while(topnSet.size() < hotDataSize && indexOne < oneHotSet.size())
					topnSet.add(oneHotSet.get(indexOne++));
				while(topnSet.size() < hotDataSize && indexTheOther < theOtherSet.size())
					topnSet.add(theOtherSet.get(indexTheOther++));
				//System.out.println(Thread.currentThread().toString() + " " + endGate.getCount());
				try {
					coordinator.put(topnSet);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					endGate.countDown();
				}
			}
		}
		private class Worker implements Runnable {
			public final File part;
			public final long timeSliceSize;
			public final PartitionTable<Long, Data> partTable = new PartitionTable<>(1000);
			public final ArrayList<Map.Entry<Long, Data>> partList;
			public Worker(File part, long timeSliceSize) {
				this.part = part;
				this.timeSliceSize = timeSliceSize;
				this.partList = null;
			}
			public Worker(ArrayList<Map.Entry<Long, Data>> partList) {
				this.part = null;
				this.timeSliceSize = 0;
				this.partList = partList;
			}
			@Override
			public void run() {
				if(part != null)
					scanFile();
				else
					scanList();
				LOGGER.info("Worker " + Thread.currentThread().getName() + " is searching hot data.");
				searchHotData();
				try {
					coordinator.put(partTable.getHotSet());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			private void scanList(){
				Iterator<Map.Entry<Long, Data>> iter = partList.iterator();
				while(iter.hasNext()) {
					Map.Entry<Long, Data> entry = iter.next();
					Data value = null;
					if((value = partTable.get(entry.getKey())) == null)
						partTable.put(entry.getKey(), entry.getValue());
					else {
						ForwRecStats status = (ForwRecStats) value;
						int timeSlice = ((ForwRecStats) entry.getValue()).getCurrTimeSlice();
						if(status.getCurrTimeSlice() != timeSlice) {
							status.updatePreviousTimeSlice();
							status.setCurrTimeSlice(timeSlice);
							status.updateFrequencyByExponentialSmoothing(true);
						}
					}
				}
			}
			private void scanFile(){
				try(BufferedReader br = new BufferedReader(new FileReader(part))) {
					for(String record = br.readLine();
							record != null;
							record = br.readLine()){
						String[] trace = record.split(" ");
						long timestamp = Long.parseLong(trace[0]);
						long blockAddress = Long.parseLong(trace[3]);
						Data value = null;
						int timeSlice = (int)((timestamp - firstTimestamp) / timeSliceSize);
						
						if((value = partTable.get(blockAddress)) == null)
							partTable.put(blockAddress, new ForwRecStats(timeSlice, blockAddress));
						else {
							ForwRecStats status = (ForwRecStats) value;
							if(status.getCurrTimeSlice() != timeSlice) {
								status.updatePreviousTimeSlice();
								status.setCurrTimeSlice(timeSlice);
								status.updateFrequencyByExponentialSmoothing(true);
							}
						}
						
					}
				} catch (FileNotFoundException fe) {
					fe.printStackTrace();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			private List<Data> searchHotData() {
				PriorityQueue<Data> maxHeap = new PriorityQueue<>(1000);
				int size = getHotDataNum(threadNum);
				Iterator<Entry<Long, Data>> iter = partTable.entrySet().iterator();
				while(iter.hasNext()) {
					Entry<Long, Data> entry = iter.next();
					maxHeap.offer(entry.getValue());
				}
				for(int i = 0; !maxHeap.isEmpty() && i < size; i++)
					partTable.putInto(maxHeap.poll());
				return partTable.getHotSet();
			}
		}
		public int getHotDataNum(int pieceNum){
			return (int)(recordNum * HOT_DATA_PERCENT) / pieceNum;
		}
		
}
