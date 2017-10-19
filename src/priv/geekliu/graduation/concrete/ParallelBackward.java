package priv.geekliu.graduation.concrete;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.input.ReversedLinesFileReader;

import priv.geekliu.graduation.classifier.ParallelBackwardTemplate;
import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.classifier.PartitionTimeStrategy;
import priv.geekliu.graduation.struct.BackRecStats;
import priv.geekliu.graduation.struct.Data;
import priv.geekliu.graduation.struct.PartitionTable;

public class ParallelBackward extends ParallelBackwardTemplate{
	private int[] lastTime;
	private Thread[] workers;
	private long[] partNum;
	private long recordNum = 1;
	private volatile static Command command = Command.Initialization;
	private static double overlappedValue = 0.0;
	private static final Object MANAGER = new Object();
	private static final PartitionStatus POISON = new PartitionStatus();
	private static BlockingQueue<PartitionStatus> statusCollector;
	private static BlockingQueue<PartitionStatus> countCollector;
	private static BlockingQueue<List<BackRecStats>> container;
	private volatile static double medianThreshold = 0.0;
	//private static volatile AtomicInteger endGate;
	private static CountDownLatch endGate;
	public ParallelBackward(PartitionTimeStrategy partition, int threadNum) {
		super(partition, threadNum);
		part = new File[threadNum];
		lastTime = new int[threadNum];
		partNum = new long[threadNum];
		workers = new Thread[threadNum];
		statusCollector = new ArrayBlockingQueue<>(threadNum);
		countCollector = new ArrayBlockingQueue<>(threadNum);
		container = new ArrayBlockingQueue<>(threadNum);
//		endGate = new AtomicInteger(threadNum);
		endGate = new CountDownLatch(threadNum);
	}

	@Override
	public void partition(File fp) {

		int currTimeSlice = 1;
		long numOfSingleSlice = ((PartitionByAccessNum) partition).getPartitionSize(fp);
		BufferedWriter[] writers = initWriters(part, threadNum);
		try (BufferedReader br = new BufferedReader(new FileReader(fp))){
			String record = null;
			while((record = br.readLine()) != null){
				String[] blockTraces = record.split(" ");	
				if(blockTraces.length != 9)
					continue;
				long blockAddress = Long.parseLong(blockTraces[3]);
				int hashValue = hash(blockAddress);
				//long blockAddress = Long.parseLong(blockTraces[3]);
//				if(recordNum % partitionInterval == 0)
//					thIndex = thIndex + 1 < threadNum ? ++thIndex : thIndex;
				if(partition instanceof PartitionByAccessNum)
					currTimeSlice = (int)(recordNum / numOfSingleSlice) + 1;
				writers[hashValue].append(record + " " + currTimeSlice + "\r\n");
				lastTime[hashValue] = currTimeSlice;
				partNum[hashValue] ++;
				recordNum++;
			}
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException ioe) {
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
	public void read(File fp) {
		
	}

	@Override
	public List<Data> process() {
		PartitionStatus status = null;
		int totalHotDataNum = (int) (recordNum * HOT_DATA_PERCENT); 
		//int partHotDataNum = totalHotDataNum / threadNum ;
		int mid = 0;
		int lBorder = 0, rBorder = threadNum - 1;
		double lValue = 0.0, rValue = 0.0;
		for(int i = 0; i < threadNum; i++) {
			workers[i] = new Thread(new Worker(part[i], (long)(partNum[i] * HOT_DATA_PERCENT), lastTime[i]));
			workers[i].start();
		}
		double[] medianCandidate = new double[threadNum];
		int tlow = 0, tup = 0;
		for(int i = 0; i < threadNum; i++) {
			try {
				status = statusCollector.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} 
			medianCandidate[i] = status.getKnth();
			tlow += status.getLowCount();
			tup += status.getUpCount();
		}
		Arrays.sort(medianCandidate);
		
		//TODO modify condition
		while (tlow - totalHotDataNum != 0 && Math.abs(tup - tlow) != 0) {
			//binary search
			if(rBorder - lBorder <= 1) {
				double average = (lValue + rValue) / 2;
				medianThreshold = average;
				if(tlow < totalHotDataNum)	
					rValue = average;
				else
					lValue = average;
			} else {
				mid = (lBorder + rBorder) / 2;
				medianThreshold = medianCandidate[mid];
				if( tlow < totalHotDataNum ) {
					rBorder = mid;
					rValue = medianThreshold;
				}
				else {
					lBorder = mid;
					lValue = medianThreshold;
				}
			}
			try {
				endGate.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			endGate = new CountDownLatch(threadNum);
			command = Command.ReportCount;
			synchronized (MANAGER) {
				MANAGER.notifyAll();
			}
			tlow = 0;
			tup = 0;
			for(int j = 0; j < threadNum; j++) {
				try {
					status = countCollector.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				tlow += status.getLowCount();
				tup += status.getUpCount();
			}
			
			//TODO modify accept distance of condition 
			if(Math.abs(tup - tlow) > 0) {
				try {
					endGate.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				endGate = new CountDownLatch(threadNum);
				command = Command.TightenBounds;
				synchronized (MANAGER) {
					MANAGER.notifyAll();
				}
			}
		}

		try {
			endGate.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		endGate = new CountDownLatch(threadNum);
		command = Command.Finallize;
		synchronized (MANAGER) {
			MANAGER.notifyAll();
		}
		List<Data> hotList = new ArrayList<>();
		for(int i = 0; i < threadNum; i++) {
			List<BackRecStats> part = null;
			try {
				part = container.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			 for(BackRecStats entry : part)
				 hotList.add(entry);
		}
		Collections.sort(hotList, new RecComparator());
		return hotList;
		
	}
	public class Worker implements Runnable, Communicatable {
		private final PartitionTable<Long, BackRecStats> table = new PartitionTable<>(100);
		private final File fp;
		private final long hotDataNum;
		private final int lastTimeSlice;
		private double kthLower = Double.MAX_VALUE;
		private double acceptThresh = 0.0;
		private int prevTimeSlice = -1;
		public Worker(File fp, long partNum, int lastTimeSlice){
			this.fp = fp;
			this.hotDataNum = partNum;
			this.lastTimeSlice = lastTimeSlice;
		}
		@Override
		public void initialize(ReversedLinesFileReader fr) {
			String record = null;
			int krecordCount = 0;
			int lowCount = 0, upCount = 0;
			try {
				while(krecordCount <= hotDataNum && (record = fr.readLine()) != null){
					String[] trace = record.split(" ");
					if(trace.length != 10)
						continue;
					long blockAddress = Long.parseLong(trace[3]);
					int timeSlice = Integer.parseInt(trace[9]);
					if(table.containsKey(blockAddress))
						continue;
					BackRecStats recStatus = new BackRecStats(blockAddress, timeSlice);
					recStatus.updateBoundingAccessFrequencyEstimate(timeSlice, lastTimeSlice);
					//TODO check first time slice
					recStatus.updateLowerEstimate(1, lastTimeSlice);
					recStatus.updateUpperEstimate(timeSlice, lastTimeSlice);
					if(recStatus.getLowerEstimateLimit() < kthLower) {
						kthLower = recStatus.getLowerEstimateLimit();
					}
						
					table.put(blockAddress, recStatus);
					krecordCount ++;
					prevTimeSlice = timeSlice;
				}
			} catch (FileNotFoundException fe) {
				fe.printStackTrace();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
			acceptThresh = lastTimeSlice - Math.log(kthLower) / Math.log(1 - Data.getAlpha());
			if(krecordCount < hotDataNum) {
				try {
					statusCollector.put(POISON);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return;
			}
			try {
				while((record = fr.readLine()) != null) {
					String[] trace = record.split(" ");
					if(trace.length != 10)
						continue;
					long blockAddress = Long.parseLong(trace[3]);
					int timeSlice = Integer.parseInt(trace[9]);
					BackRecStats recStatus = table.get(blockAddress);
					if(recStatus == null) {
						//TODO test it and debug
						if(timeSlice <= acceptThresh)
							continue;
						else {
							recStatus = new BackRecStats(blockAddress, timeSlice);
							recStatus.updateBoundingAccessFrequencyEstimate(timeSlice, lastTimeSlice);
							recStatus.updateLowerEstimate(1, lastTimeSlice);
							recStatus.updateUpperEstimate(timeSlice, lastTimeSlice);
//							if(recStatus.getLowerEstimateLimit() < kthLower)
//								kthLower = recStatus.getLowerEstimateLimit();
							table.put(blockAddress, recStatus);
						}
					} else if (recStatus.getTimeSlice() != timeSlice) {
						recStatus.setTimeSlice(timeSlice);
						recStatus.updateBoundingAccessFrequencyEstimate(timeSlice, lastTimeSlice);
					}
					
					if( prevTimeSlice != timeSlice ) {
						BackRecStats value = null;
						kthLower = Double.MAX_VALUE;
						Iterator<Map.Entry<Long, BackRecStats>> iter = table.entrySet().iterator();
						PriorityQueue<BackRecStats> heap = new PriorityQueue<>();
						while(iter.hasNext()){
							 value = iter.next().getValue();
							 value.updateLowerEstimate(1, lastTimeSlice);
							 value.updateUpperEstimate(timeSlice, lastTimeSlice);
							 heap.add(value);
//							 if( value.getLowerEstimateLimit() < kthLower )
//								 kthLower = value.getLowerEstimateLimit();
						}

						for(int i = 0; !heap.isEmpty() && i < hotDataNum; i++)
							kthLower = heap.poll().getLowerEstimateLimit();
						
						iter = table.entrySet().iterator();
						while(iter.hasNext()) {
							value = iter.next().getValue();
							if( value.getUpperEstimateLimit() <= kthLower )
								iter.remove();
						}
						LOGGER.info(Thread.currentThread().getName() + 
								"[Initialize] Table size: " + table.size() + 
								" Absolute value of HD size from GD size: " + 
								Math.abs(table.size() - hotDataNum));
						//TODO check condition
						if(Math.abs(table.size() - hotDataNum) < 10) {
							//knth = klowRec.getLowEstimateLimit();
							Iterator<Map.Entry<Long, BackRecStats>> it = table.entrySet().iterator();
							BackRecStats entry = null;
							while(it.hasNext()){
								entry = it.next().getValue();
								if(entry.getLowerEstimateLimit() > kthLower)
									lowCount ++;
								if(entry.getUpperEstimateLimit() > kthLower)
									upCount ++;
							}
							PartitionStatus ret = new PartitionStatus(kthLower, lowCount, upCount);
							try {
								statusCollector.put(ret);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							return;
						}
						acceptThresh = lastTimeSlice - Math.log(kthLower) / Math.log(1 - Data.getAlpha());
					}
					prevTimeSlice = timeSlice;
				}
			} catch (FileNotFoundException fe) {
				fe.printStackTrace();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
			try {
				statusCollector.put(POISON);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void report() {
			int lowCount = 0, upCount = 0;
			double minOverValue = 0.0;
			Iterator<Map.Entry<Long, BackRecStats>> iter = table.entrySet().iterator();
			BackRecStats value = null;
			overlappedValue = Double.MAX_VALUE;
			while(iter.hasNext()) {
				value = iter.next().getValue();
				if((minOverValue = value.getUpperEstimateLimit() - medianThreshold) > 0 &&
						minOverValue < overlappedValue)
					overlappedValue = minOverValue;
					
				if(value.getLowerEstimateLimit() > medianThreshold)
					lowCount ++;
				if(value.getUpperEstimateLimit() > medianThreshold)
					upCount ++;
			}
			PartitionStatus countResult = new PartitionStatus(lowCount, upCount);
			try {
				countCollector.put(countResult);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void tightenBounds(ReversedLinesFileReader fr) {
			acceptThresh = lastTimeSlice - Math.log(medianThreshold) / Math.log(1 - Data.getAlpha());
			double timeSliceBounds = lastTimeSlice + 1 - Math.log(overlappedValue) / Math.log(1 - Data.getAlpha());
			String record = null;
			try {
				while((record = fr.readLine()) != null) {
					String[] trace = record.split(" ");
					if(trace.length != 10)
						continue;
					long blockAddress = Long.parseLong(trace[3]);
					int timeSlice = Integer.parseInt(trace[9]);
					BackRecStats recStatus = table.get(blockAddress);
					if(recStatus == null) {
						//TODO test it and debug
						if(timeSlice <= acceptThresh)
							continue;
						else {
							recStatus = new BackRecStats(blockAddress, timeSlice);
							recStatus.updateBoundingAccessFrequencyEstimate(timeSlice, lastTimeSlice);
							recStatus.updateLowerEstimate(1, lastTimeSlice);
							recStatus.updateUpperEstimate(timeSlice, lastTimeSlice);
							table.put(blockAddress, recStatus);
						}
					} else if (recStatus.getTimeSlice() != timeSlice) {
						recStatus.setTimeSlice(timeSlice);
						recStatus.updateBoundingAccessFrequencyEstimate(timeSlice, lastTimeSlice);
					}
					
					if( prevTimeSlice != timeSlice ) {
						BackRecStats value = null;
						kthLower = Double.MAX_VALUE;
						Iterator<Map.Entry<Long, BackRecStats>> iter = table.entrySet().iterator();
						PriorityQueue<BackRecStats> heap = new PriorityQueue<>();
						while(iter.hasNext()){
							 value = iter.next().getValue();
							 value.updateLowerEstimate(1, lastTimeSlice);
							 value.updateUpperEstimate(timeSlice, lastTimeSlice);
							 heap.add(value);
						}
						
						for(int i = 0; !heap.isEmpty() && i < hotDataNum; i++)
							kthLower = heap.poll().getLowerEstimateLimit();
						iter = table.entrySet().iterator();
						
						while(iter.hasNext()) {
							value = iter.next().getValue();
							if( value.getUpperEstimateLimit() <= kthLower )
								iter.remove();
						}
						LOGGER.info("[Tighten] Table size: " + table.size() + 
								" Absolute value of HD size from GD size: " + Math.abs(table.size() - hotDataNum));
						if(timeSlice <= timeSliceBounds) 
							return;
					}
					prevTimeSlice = timeSlice;
				}
			} catch (FileNotFoundException fe) {
				fe.printStackTrace();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}

		@Override
		public void shutDown() {
			System.out.println("Last Slice: " + prevTimeSlice);
			Iterator<Map.Entry<Long, BackRecStats>> iter = table.entrySet().iterator();
			Map.Entry<Long, BackRecStats> entry = null;
			while(iter.hasNext()) {
				entry = iter.next();
				if(entry.getValue().getUpperEstimateLimit() > medianThreshold)
					table.putInto(entry.getValue());
			}
			try {
				container.put(table.getHotSet());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			try(ReversedLinesFileReader fr = new ReversedLinesFileReader(fp, Charset.forName("UTF-8"))){
				while(true) {
					switch (command) {
					case Initialization:
						initialize(fr);
						break;
					case ReportCount:
						report();
						break;
					case TightenBounds:
						tightenBounds(fr);
						break;
					case Finallize:
						shutDown();
						break;
					}
					synchronized (MANAGER) {
						if(command == Command.Finallize)
							break;
						//endGate.decrementAndGet();
						endGate.countDown();
						MANAGER.wait();				
					}
				}
			} catch (FileNotFoundException fe) {
				fe.printStackTrace();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	private int hash(long blockAddress) {
		return Math.abs(String.valueOf(blockAddress).hashCode() % threadNum);
	}
}
