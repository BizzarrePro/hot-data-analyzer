package priv.geekliu.graduation.classifier;

import java.io.File;

public abstract class PartitionTimeStrategy {
	protected int partitionNum = 0;
	
	public PartitionTimeStrategy(int partitionNum){
		if(partitionNum < 5)
			this.partitionNum = 5;
		else
			this.partitionNum = partitionNum;
	}
	
	public int getPartitionNum(){
		return partitionNum;
	}
	
	public abstract long getPartitionSize(File fp);
}
