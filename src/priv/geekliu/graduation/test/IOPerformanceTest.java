package priv.geekliu.graduation.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.input.ReversedLinesFileReader;

public class IOPerformanceTest {
	public static void main(String[] args) throws IOException{
		readForward();
//		readBackward();
//		testWritePerf();
//		testRandomWritePerf();
	}
	public static void testRandomWritePerf() throws IOException {
		int number = 10;
		BufferedWriter[] writer = new BufferedWriter[number];
		long start = System.currentTimeMillis();
		for(int i = 0; i < number; i++) {
			String fileName = "garbage" + i + ".blkparse";
			writer[i] = new BufferedWriter(new FileWriter(new File("J:\\data\\" + fileName)));
		}
		long i = 0;
		int count = 0;
		int hash = 0;
		while(i < 220000000) {
			char ch = (char)(Math.random() * 200);
			writer[hash].write(ch);
			if(count == 75) {
				count = 0;
				hash = ch % number;
			}
			count ++ ;
			i++;
		}
		for(int j = 0; j < number; j++)
			writer[j].close();
		System.out.println(System.currentTimeMillis() - start);
	}
	public static void readBackward(){
		long start = System.currentTimeMillis();
		long num = 0;
		try(ReversedLinesFileReader fr = new ReversedLinesFileReader(new File("J:\\data\\part5.blkparse"),  Charset.forName("UTF-8"))){
			String str = null;
			while((str = fr.readLine()) != null){
				str = fr.readLine();
				num += str.length();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(System.currentTimeMillis() - start);
		System.out.println(num);
	}
	public static void testWritePerf() throws IOException {
		long start = System.currentTimeMillis();
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("J:\\data\\garbage.blkparse")));
		long i = 0;
		while(i < 220000000) {
			char ch = (char)(Math.random() * 200);
			bw.write(ch);
			i++;
		}
		bw.flush();
		bw.close();
		System.out.println(System.currentTimeMillis() - start);
		File f = new File("J:\\data\\garbage.blkparse");
		f.delete();
	}
	public static void readForward() throws IOException{
		long start = System.currentTimeMillis();
		long num = 0;
	//	BufferedWriter bw = new BufferedWriter(new FileWriter(new File("J:\\data\\garbage.blkparse")), 131072);
		try(BufferedReader bf = new BufferedReader(new FileReader(new File("J:\\data\\trace1.blkparse")))) {
			String str = null;
			while((str = bf.readLine()) != null){
				num += str.length();
			}
		} catch (IOException e){
			e.printStackTrace();
		}
//		bw.flush();
//		bw.close();
//		File f = new File("J:\\data\\garbage.blkparse");
//		f.delete();
		System.out.println(System.currentTimeMillis() - start);
		System.out.println(num);
	}
}
