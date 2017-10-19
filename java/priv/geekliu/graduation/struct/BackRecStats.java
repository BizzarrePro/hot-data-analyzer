package priv.geekliu.graduation.struct;


public class BackRecStats extends Data{
	private int timeSlice = 0;
	private double lowerEstimateLimit = 0;
	private double upperEstimateLimit = 0;
	public BackRecStats(long blockAddress, int timeSlice) {
		super(blockAddress);
		this.timeSlice = timeSlice;
	}
	public void setTimeSlice(int timeSlice) { 
		this.timeSlice = timeSlice;
	}
	public int getTimeSlice() {
		return timeSlice;
	}
	public double getLowerEstimateLimit() {
		return lowerEstimateLimit;
	}
	public void setLowerEstimateLimit(double lowerEstimateLimit) {
		this.lowerEstimateLimit = lowerEstimateLimit;
	}
	public double getUpperEstimateLimit() {
		return upperEstimateLimit;
	}
	public void setUpperEstimateLimit(double upperEstimateLimit) {
		this.upperEstimateLimit = upperEstimateLimit;
	}
	public void updateBoundingAccessFrequencyEstimate(int currTimeSlice, int endTimeSlice){
		frequencyEstimate = frequencyEstimate + ALPHA * Math.pow((1 - ALPHA), endTimeSlice - currTimeSlice);
	}
	public void updateUpperEstimate(int currTimeSlice, int endTimeSlice){
		upperEstimateLimit = frequencyEstimate + Math.pow(1 - ALPHA, endTimeSlice - currTimeSlice + 1);
	}
	public void updateLowerEstimate(int startTimeSlice, int endTimeSlice){
		lowerEstimateLimit = frequencyEstimate + Math.pow(1 - ALPHA, endTimeSlice - startTimeSlice + 1);
	}
	@Override
	public int compareTo(Data o) {
		//TODO debug
		double lowerEst = ((BackRecStats) o).lowerEstimateLimit;
		if(lowerEstimateLimit == lowerEst)
			return 0;
		return lowerEstimateLimit < lowerEst ? 1 : -1;
	}

//	public String toString() {
//		return super.toString() + " Lower: " + lowerEstimateLimit + " Upper: " + upperEstimateLimit;
//	}
}
