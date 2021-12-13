package de.tub.dima.scotty.slicing.slice;


import de.tub.dima.scotty.slicing.state.AggregateState;

public class RunningSlice<InputType> {

    private long tStart;
    private long pStart;
    private long tEnd;
    private long pEnd;

    private AggregateState<InputType> state;

    public RunningSlice(long startTs, long endTs) {
        this.tStart = startTs;
        this.pStart = startTs;
        this.tEnd = endTs;
        this.pEnd = endTs;
        this.state = null;
    }

    /**
     * @return slice start timestamp
     */
    public long getTStart() {
        return tStart;
    }

    /**
     * @return last slice start timestamp
     */
    public long getPStart() {
        return pStart;
    }

    /**
     * @return slice end timestamp
     */
    public long getTEnd() {
        return tEnd;
    }

    /**
     * @return last slice end timestamp
     */
    public long getPEnd() {
        return pEnd;
    }

    /**
     * @return True if preaggregate can be reused otherwise False
     */
    public boolean noPreagg() {
        if (pStart < tStart && tStart < pEnd)
            return false;

        return true;
    }

    public void moveTo(long tStart, long tEnd) {
        this.pStart = this.tStart;
        this.tStart = tStart;
        this.pEnd = this.tEnd;
        this.tEnd = tEnd;
    }

    public boolean add(Slice<InputType, ?> slice) {
        if (slice.getTEnd() <= this.pEnd)
            return true;

        if (this.tStart <= slice.getTStart() && slice.getTEnd() <= this.tEnd)
            this.state.merge(slice.getAggState());

        return false;
    }

    public boolean rm(Slice<InputType, ?> slice) {
        if (this.pStart <= slice.getTStart() && slice.getTEnd() <= this.tStart) {
            state.invert(slice.getAggState());
            return false;
        }

        return true;
    }

    public AggregateState<InputType> getAggState() {
        return state;
    }

    public void setState(AggregateState<InputType> state) {
        this.state = state;
    }

    public String toString() {
        return "Slice{" +
                "tStart=" + tStart +
                ", pStart=" + pStart +
                ", tEnd=" + tEnd +
                ", pEnd=" + pEnd +
                '}';
    }
}
