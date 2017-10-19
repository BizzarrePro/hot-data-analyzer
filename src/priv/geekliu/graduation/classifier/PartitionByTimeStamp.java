package priv.geekliu.graduation.classifier;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class PartitionByTimeStamp extends PartitionTimeStrategy{
	private static final long MAPPING_LIMIT = 1000;
	private long timeSliceSize = -1;
	private long timeStampSpan = -1;
	private long firstTimeStamp = -1;
	private long lastTimeStamp = -1;
	public PartitionByTimeStamp(int partitionNum) {
		super(partitionNum);
	}
	public long getTimeStampSpan(){
		return timeStampSpan;
	}
	public long getStartTimeStamp() {
		return firstTimeStamp;
	}
	public long getLastTimeStamp() {
		return lastTimeStamp;
	}
	@Override
	public long getPartitionSize(File fp) {
		if(timeSliceSize == -1) {
			try(BufferedReader br = new BufferedReader(new FileReader(fp))){
				lastTimeStamp = getLastDataTimeStamp(fp);
				String record = null;
				while((record = br.readLine()) != null) {
					String[] trace = record.split(" ");
					if(trace.length != 9)
						continue;
					firstTimeStamp = Long.parseLong(trace[0]);
					timeStampSpan = lastTimeStamp - firstTimeStamp;
					timeSliceSize = timeStampSpan / partitionNum;
					break;
				}
			} catch (FileNotFoundException fe){
				fe.printStackTrace();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		return timeSliceSize;
	}
	/*
	 * Use memory mapping file for searching last time stamp, 
	 * Memory mapping file is much faster than random access file.
	 * This step requires operating system coordination that 
	 * supports virtual memory.
	*/
	private long getLastDataTimeStamp(File trace) throws IOException{
		FileChannel  channel = null;
		String[] lastTrace = null;
		StringBuffer record = new StringBuffer();
		try {
			channel = FileChannel.open(trace.toPath());
			long size = channel.size();
			long mappingSize = size > MAPPING_LIMIT ? MAPPING_LIMIT : size;
			MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY,
					size - mappingSize, mappingSize);
			for(int index = buffer.capacity() - 1; ; index--) {
				byte b = buffer.get(index);
				if((char)b == '\n'){
					lastTrace = record.reverse().toString().split(" ");
					if(lastTrace.length == 9)
						break;
					record = new StringBuffer();
				}
				record.append((char)b);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			channel.close();
		}
		return Long.parseLong(lastTrace[0]);
	}
}
