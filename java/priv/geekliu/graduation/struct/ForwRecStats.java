package priv.geekliu.graduation.struct;


public class ForwRecStats extends Data{
	private int currTimeSlice = 1;
	private int prevTimeSlice = 1;
	public ForwRecStats(int timeSlice, long blockAddress) {
		super(blockAddress);
		this.currTimeSlice = timeSlice;
		this.prevTimeSlice = timeSlice;
	}
	public int getCurrTimeSlice() {
		return currTimeSlice;
	}
	public void setCurrTimeSlice(int currTimeSlice) {
		this.currTimeSlice = currTimeSlice;
	}
	public int getPrevTimeSlice() {
		return prevTimeSlice;
	}
	public void setPrevTimeSlice(int prevTimeSlice) {
		this.prevTimeSlice = prevTimeSlice;
	}
	public void updateFrequencyByExponentialSmoothing(boolean appeared){
		int exponent = currTimeSlice - prevTimeSlice;
		double decayFactor = Math.pow((1 - ALPHA), exponent);
		if(appeared) 
			frequencyEstimate = ALPHA + frequencyEstimate * decayFactor;
		else 
			frequencyEstimate = frequencyEstimate * decayFactor;
	}
	public void accumulatePartEstimate(long factor){
		frequencyEstimate = frequencyEstimate +  Math.pow(1 - ALPHA, factor);
	}
	
	public void obtainOverallFrequency(int timeSlice, double prevEstimate){
		frequencyEstimate = ALPHA * frequencyEstimate + Math.pow(1 - ALPHA, timeSlice) * prevEstimate;
	}
	public void updatePreviousTimeSlice(){
		this.prevTimeSlice = this.currTimeSlice;
	}	
}
