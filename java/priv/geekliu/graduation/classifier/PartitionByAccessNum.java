package priv.geekliu.graduation.classifier;

import java.io.File;

public class PartitionByAccessNum extends PartitionTimeStrategy{
	private long accessRecNum = -1;
	public PartitionByAccessNum(int partitionNum) {
		super(partitionNum);
	}

	@Override
	public long getPartitionSize(File fp) {
		if(accessRecNum == -1) {
			accessRecNum = getEstimatedNumOfLines(fp) / partitionNum;
			return accessRecNum;
		}
		return accessRecNum;
	}
	/**
	 * The file header size is estimated to be 1024, with an average of 75 bytes per trace
	 * @return the number of file lines for computing the number of hot data 
	 */
	private long getEstimatedNumOfLines(File fp){
		long size = fp.length();
		return (size - 1024) / 75;
	}
}
