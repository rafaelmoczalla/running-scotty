package de.tub.dima.scotty.core;

import de.tub.dima.scotty.core.windowType.*;

public interface WindowCollector {

    public void trigger(Integer id, boolean overlapping, long start, long end, WindowMeasure measure);
}
