package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;

public interface ContextFreeWindow extends Window {

    long assignNextWindowStart(long position);

    void triggerWindows(Integer id, boolean overlapping, WindowCollector aggregateWindows, long lastWatermark, long currentWatermark);

    long clearDelay();
}
