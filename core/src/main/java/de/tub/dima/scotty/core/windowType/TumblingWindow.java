package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.*;

public class TumblingWindow implements ContextFreeWindow {

    private final WindowMeasure measure;
    /**
     * Size of the tumbling window
     */
    private final long size;

    public TumblingWindow(WindowMeasure measure, long size) {
        this.measure = measure;
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    @Override
    public boolean isOverlapping() {
        return false;
    }

    @Override
    public long assignNextWindowStart(long recordStamp) {
        return recordStamp + getSize() - (recordStamp) % getSize();
    }

    @Override
    public void triggerWindows(Integer id, boolean overlapping, WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
        long lastStart = lastWatermark - ((lastWatermark + size) % size);
        for (long windowStart = lastStart; windowStart + size <= currentWatermark; windowStart += size) {
            aggregateWindows.trigger(id, overlapping, windowStart, windowStart + size, measure);
        }
    }

    @Override
    public long clearDelay() {
        return size;
    }

    @Override
    public String toString() {
        return "TumblingWindow{" +
                "measure=" + measure +
                ", size=" + size +
                '}';
    }
}
