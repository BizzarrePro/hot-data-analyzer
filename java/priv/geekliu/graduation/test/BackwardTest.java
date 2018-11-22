package priv.geekliu.graduation.test;

import java.io.File;
import java.util.Collections;
import java.util.List;

import priv.geekliu.graduation.classifier.AbstractClassifier;
import priv.geekliu.graduation.classifier.AbstractClassifier.ParallelOption;
import priv.geekliu.graduation.classifier.AbstractClassifier.ReadMode;
import priv.geekliu.graduation.classifier.AbstractClassifier.SerialOption;
import priv.geekliu.graduation.classifier.PartitionByAccessNum;
import priv.geekliu.graduation.concrete.ParallelBackward;
import priv.geekliu.graduation.concrete.SerialBackward;
import priv.geekliu.graduation.struct.Data;

public class BackwardTest {
	private static AbstractClassifier ac = null;
	public static void main(String[] args) throws Exception {
		ac = AbstractClassifier.invokeClassifier(SerialOption.SERIAL_BACKWARD, 1000);
//		ac = AbstractClassifier.invokeClassifier(ParallelOption.PARALLEL_BACKWARD, 1000, 4);
		List<Data> result = ac.classify(new File("J:\\data\\part1.blkparse"));
		for(Data d : result)
			System.out.println(d);

		System.out.println("Hello World");
	}
}
