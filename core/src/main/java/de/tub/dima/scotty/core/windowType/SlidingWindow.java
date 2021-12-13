package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;

public class SlidingWindow implements ContextFreeWindow {

    private final WindowMeasure measure;

    /**
     * Size of the sliding window
     */
    private final long size;

    /**
     * The window slide step
     */
    private final long slide;

    public SlidingWindow(WindowMeasure measure, long size, long slide) {
        this.measure = measure;
        this.size = size;
        this.slide = slide;
    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    @Override
    public boolean isOverlapping() {
        if (this.slide < this.size)
            return true;

        return false;
    }


    @Override
    public long assignNextWindowStart(long recordStamp) {
        return recordStamp + getSlide() - (recordStamp) % getSlide();
    }

    public static long getWindowStart(long timestamp, long windowSize, long windowSlide) {
        return timestamp - ((timestamp  + windowSlide) % windowSlide) - windowSize;
    }

    @Override
    public void triggerWindows(Integer id, boolean overlapping, WindowCollector collector, long lastWatermark, long currentWatermark) {
        long lastStart  = getWindowStart(currentWatermark, size, slide);
        long firstStart = getWindowStart(lastWatermark, size, slide) + slide;

        //for (long windowStart = lastStart; windowStart + size > lastWatermark; windowStart -= slide) {
        for (long windowStart = firstStart; windowStart <= lastStart; windowStart += slide) {
            if (windowStart>=0 && windowStart + size <= currentWatermark + 1)
                collector.trigger(id, overlapping, windowStart, windowStart + size, measure);
        }
    }

    @Override
    public long clearDelay() {
        return size;
    }

    @Override
    public String toString() {
        return "SlidingWindow{" +
                "measure=" + measure +
                ", size=" + size +
                ", slide=" + slide +
                '}';
    }
}
