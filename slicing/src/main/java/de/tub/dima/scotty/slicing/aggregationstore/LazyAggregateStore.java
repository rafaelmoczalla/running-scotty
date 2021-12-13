package de.tub.dima.scotty.slicing.aggregationstore;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.slice.RunningSlice;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.state.AggregateWindowState;
import de.tub.dima.scotty.state.StateFactory;

import java.util.ArrayList;
import java.util.HashMap;

import org.w3c.dom.ls.LSException;

/**
 * A lazy in memory aggregation store implementation.
 * All slices are stored in a plain java array.
 * @param <InputType>
 */
public class LazyAggregateStore<InputType> implements AggregationStore<InputType> {

    private final SliceList slices = new SliceList();
    private HashMap<Integer, RunningSlice<InputType>> runningAggs = new HashMap<>();

    @Override
    public Slice<InputType, ?> getCurrentSlice() {
        return slices.get(slices.size() - 1);
    }

    @Override
    public int findSliceIndexByTimestamp(long ts) {
        for (int i = size() - 1; i >= 0; i--) {
            Slice<InputType, ?> currentSlice = this.getSlice(i);
            if (currentSlice.getTStart() <= ts) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int findSliceIndexByCount(long count) {
        for (int i = size() - 1; i >= 0; i--) {
            Slice<InputType, ?> currentSlice = this.getSlice(i);
            if (currentSlice.getCStart() <= count) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public Slice<InputType, ?> getSlice(int index) {
        return this.slices.get(index);
    }


    @Override
    public void insertValueToCurrentSlice(InputType element, long ts) {
        this.getCurrentSlice().addElement(element, ts);
    }

    @Override
    public void insertValueToSlice(int index, InputType element, long ts) {
        this.getSlice(index).addElement(element, ts);
    }

    @Override
    public void appendSlice(Slice<InputType, ?> newSlice) {
        this.slices.add(newSlice);
    }

    @Override
    public int size() {
        return this.slices.size();
    }

    @Override
    public boolean isEmpty() {
        return this.slices.isEmpty();
    }

    @Override
    public void aggregate(WindowManager.AggregationWindowCollector aggregateWindows, boolean invertible, long maxOptLateness, long minTs, long maxTs, long minCount, long maxCount) {

        // start index = 0 || minTS
        int startIndex = Math.max(findSliceIndexByTimestamp(minTs),0);
        startIndex = Math.min(startIndex, findSliceIndexByCount(minCount));
        // endIndex = this.size()-1 || maxTs
        int endIndex = Math.min(this.size() - 1, findSliceIndexByTimestamp(maxTs));
        endIndex = Math.max(endIndex, findSliceIndexByCount(maxCount));

        for (AggregateWindow<?> window : aggregateWindows) {
            AggregateWindowState ws = (AggregateWindowState) window;

            if(invertible && ws.isOverlapping()) {
                startIndex = Math.max(findSliceIndexByTimestamp(minTs - maxOptLateness),0);
                startIndex = Math.min(startIndex, findSliceIndexByCount(minCount));

                RunningSlice rpa;

                if (!runningAggs.containsKey(ws.getId()))
                    runningAggs.put(ws.getId(), new RunningSlice<>(ws.getStart(), ws.getEnd()));

                rpa = runningAggs.get(ws.getId());

                rpa.moveTo(ws.getStart(), ws.getEnd());

                // check if we can reuse a preaggreagte
                if (rpa.noPreagg()) {
                    // compute agg as usual
                    for (int i = startIndex; i <= endIndex; i++) {
                        Slice<InputType, ?> currentSlice = getSlice(i);

                        if(ws.containsSlice(currentSlice)){
                            ws.addState(currentSlice.getAggState());
                        }
                    }

                    rpa.setState(ws.getState());
                } else {
                    // remove slices that are moved out of the window anymore
                    for (int i = startIndex; i <= endIndex; i++) {
                        Slice<InputType, ?> currentSlice = getSlice(i);

                        // remove slice from preaggregate and stop if all slices that need to be
                        // removed before start are removed
                        if(rpa.rm(currentSlice))
                            break;
                    }

                    // ergo skip all slices in the middle

                    // add new slices that moved into the window
                    for (int i = endIndex; startIndex <= i; i--) {
                        Slice<InputType, ?> currentSlice = getSlice(i);

                        // remove slice from preaggregate and stop if all slices that need to be
                        // removed before start are removed
                        if(rpa.add(currentSlice))
                            break;
                    }
                }

                ws.addState(rpa.getAggState());
            } else {
                for (int i = startIndex; i <= endIndex; i++) {
                    Slice<InputType, ?> currentSlice = getSlice(i);

                    if(ws.containsSlice(currentSlice)){
                        ws.addState(currentSlice.getAggState());
                    }
                }
            }
        }

        //Slice currentSlice = getCurrentSlice();
        //for (AggregateWindow window : aggregateWindows) {
        //    AggregateWindowState ws = (AggregateWindowState) window;
        //    if (ws.getTEnd() > currentSlice.getTStart()) {
        //        ws.addState(currentSlice.getAggState());
        //    }
        //}
        //System.out.println(this.slices.size());

    }

    @Override
    public void addSlice(int index, Slice<InputType, ?> newSlice) {
        this.slices.add(index, newSlice);
    }

    @Override
    public void mergeSlice(int sliceIndex) {
        Slice<InputType, ?> sliceA = this.getSlice(sliceIndex);
        Slice<InputType, ?> sliceB = this.getSlice(sliceIndex+1);
        sliceA.merge(sliceB);
        this.slices.remove(sliceIndex+1);
    }

    @Override
    public int findSliceByEnd(long start) {
        for (int i = size() - 1; i >= 0; i--) {
            Slice<InputType, ?> currentSlice = this.getSlice(i);
            if (currentSlice.getTEnd()==start) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void removeSlices(final long maxTimestamp) {
        final int index = this.findSliceIndexByTimestamp(maxTimestamp);

        if (index <= 0)
            return;
        this.slices.removeRange(0, index);
        //System.out.println("remove");

    }

    private class SliceList extends ArrayList<Slice<InputType, ?>> {
        public SliceList() {
            super(1000);
        }

        @Override
        public void removeRange(final int fromIndex, final int toIndex) {
            super.removeRange(fromIndex, toIndex);
        }
    }
}
