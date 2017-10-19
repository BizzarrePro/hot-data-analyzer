package priv.geekliu.graduation.test;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import priv.geekliu.graduation.classifier.AbstractClassifier;
import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.concrete.OptimizedSerialForward;
import priv.geekliu.graduation.concrete.SerialForward;
import priv.geekliu.graduation.struct.Data;

public class CoincidenceDetection {
	private static AbstractClassifier ac = null;
	public static void main(String[] args) {
		ac = new OptimizedSerialForward(new PartitionByAccessNum(5000));
		List<Data> optimized = ac.classify(new File("J:\\data\\trace1.blkparse"));
		ac = new SerialForward(new PartitionByAccessNum(5000));
		List<Data> original = ac.classify(new File("J:\\data\\trace1.blkparse"));
		Set<Long> pool = new HashSet<>();
		for(Data d : original)
			pool.add(d.getBlockAddress());
		long numerator = 0l;
		for(Data d : optimized)
			if(pool.contains(d.getBlockAddress()))
				numerator ++;
		System.out.println(numerator + " " + original.size() + " " + optimized.size());
//		System.out.println((double)numerator / (double)original.size());
	}
}
