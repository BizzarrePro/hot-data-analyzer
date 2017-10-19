package priv.geekliu.graduation.test;

import java.io.File;
import java.util.List;

import priv.geekliu.graduation.classifier.AbstractClassifier;
import priv.geekliu.graduation.classifier.AbstractClassifier.ParallelOption;
import priv.geekliu.graduation.classifier.AbstractClassifier.SerialOption;
import priv.geekliu.graduation.classifier.PartitionByTimeStamp;
import priv.geekliu.graduation.classifier.PartitionTimeStrategy;
import priv.geekliu.graduation.classifier.AbstractClassifier.ReadMode;
import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.concrete.OptimizedSerialForward;
import priv.geekliu.graduation.concrete.ParallelForwardPartByRecId;
import priv.geekliu.graduation.concrete.ParallelForwardPartByTimeStmp;
import priv.geekliu.graduation.concrete.SerialForward;
import priv.geekliu.graduation.struct.Data;

public class ForwardTest {
	private static AbstractClassifier ac = null;
	public static void main(String[] args) throws Exception {
//		ac = AbstractClassifier.invokeClassifier(SerialOption.SERIAL_FORWARD, 1000);
//		ac = AbstractClassifier.invokeClassifier(SerialOption.SERIAL_OPTIMIZED_FORWARD, 100);
//		ac = AbstractClassifier.invokeClassifier(ParallelOption.PARALLEL_FORWARD_WITH_RECORD_ID_PARTITION, 100, 4);
		ac = AbstractClassifier.invokeClassifier(ParallelOption.PARALLEL_FORWARD_WITH_TIME_SLICE_PARTITION, 100, 4);
		List<Data> result = ac.classify(new File("J:\\data\\part1.blkparse"));
//		List<Data> result = ac.classify(new File("J:\\data\\part1.blkparse"), ReadMode.READ_IN_MEMORY_DIRECTLY);
		for(Data d : result)
			System.out.println(d);
	}
}
