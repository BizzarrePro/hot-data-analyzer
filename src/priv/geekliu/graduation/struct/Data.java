package priv.geekliu.graduation.struct;

public abstract class Data implements Comparable<Data>{
	protected long blockAddress = 0;
	protected double frequencyEstimate = 0;
	protected  static final double ALPHA = 0.05;
	public Data(long blockAddress) {
		this.blockAddress = blockAddress;
	}
	public long getBlockAddress() {
		return blockAddress;
	}
	public void setBlockAddress(long blockAddress) {
		this.blockAddress = blockAddress;
	}
	public double getFrequencyEstimate() {
		return frequencyEstimate;
	}
	public void setFrequencyEstimate(double frequencyEstimate) {
		this.frequencyEstimate = frequencyEstimate;
	}
	public static double getAlpha(){
		return ALPHA;
	}
	@Override
	public int compareTo(Data o) {
		double esFreq = o.getFrequencyEstimate();
		if(frequencyEstimate == esFreq)
			return 0;
		return frequencyEstimate < esFreq ? 1 : -1;
	}
	public String toString(){
		return "Address: " + blockAddress + " EstimatedFrequency: " + frequencyEstimate;
	}
}
