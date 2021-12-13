package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;

import java.io.*;

public interface Window extends Serializable {
    final Integer id = 0;

    WindowMeasure getWindowMeasure();
    boolean isOverlapping();
}
