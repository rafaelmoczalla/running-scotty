package de.tub.dima.scotty.slicing.aggregationstore.test;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.ForwardContextAware;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.core.windowType.windowContext.WindowContext;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.slicing.aggregationstore.AggregationStore;
import de.tub.dima.scotty.slicing.aggregationstore.LazyAggregateStore;
import de.tub.dima.scotty.slicing.slice.EagerSlice;
import de.tub.dima.scotty.slicing.slice.LazySlice;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.slice.SliceFactory;
import de.tub.dima.scotty.state.StateFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SliceFactoryTest {

    /*
     * This test shows, that the implementation of the SliceFactory reflects
     * the decision tree for storing individual tuples in the General Stream Slicing Paper.
     */

    AggregationStore<Integer> aggregationStore;
    StateFactory stateFactory;
    WindowManager windowManager;
    SliceFactory<Integer, Integer> sliceFactory;

    @Before
    public void setup() {
        aggregationStore = new LazyAggregateStore<>();
        stateFactory = new StateFactoryMock();
        windowManager = new WindowManager(true, stateFactory, aggregationStore);
        sliceFactory = new SliceFactory<>(windowManager, stateFactory);
        windowManager.addAggregation(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
                return partialAggregate1 + partialAggregate2;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });

    }

    /**
     * Lazy slices should be produced for context-aware window types to keep the tuples
     * of the stream in memory.
     */
    @Test
    public void LazySliceTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        assertTrue(windowManager.getMaxLateness() > 0); // out-of-order stream
        assertTrue(windowManager.hasContextAwareWindow()); // no context free or session window
        assertFalse(windowManager.isSessionWindowCase());

        Slice<Integer, Integer> slice = sliceFactory.createSlice(0, 10,   new Slice.Fixed());

        assertTrue("Slice factory produced Eager Slice", slice instanceof LazySlice);
    }

    /**
     * Lazy slices should be produced for window types with count-based measure to keep the tuples
     * of the stream in memory.
     */
    @Test
    public void LazySliceTestCount() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Count));

        assertTrue(windowManager.hasCountMeasure());

        Slice<Integer, Integer> slice = sliceFactory.createSlice(0, 10,   new Slice.Fixed());

        assertTrue("Slice factory produced Eager Slice", slice instanceof LazySlice);
    }

    /**
     * Session windows do not require keeping tuples in memory, thus the SliceFactory should produce eager slices.
     */
    @Test
    public void EagerSliceTestSession() {
        windowManager.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 1000));

        assertTrue(windowManager.getMaxLateness() > 0); // out-of-order stream
        assertTrue(windowManager.hasContextAwareWindow()); // no context free window
        assertTrue(windowManager.isSessionWindowCase()); // but special case of only session window
        assertFalse(windowManager.hasCountMeasure()); // no count measure

        Slice<Integer, Integer> slice = sliceFactory.createSlice(0, 10,   new Slice.Fixed());

        assertTrue("Slice factory produced Lazy Slice", slice instanceof EagerSlice);

        windowManager.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 2000)); //add another session window
        assertTrue(windowManager.isSessionWindowCase()); // but special case of only session window

        slice = sliceFactory.createSlice(0, 10,   new Slice.Fixed());

        assertTrue("Slice factory produced Lazy Slice", slice instanceof EagerSlice);
    }

    /**
     * Session windows do not require keeping tuples in memory, but other context aware windows require to keep them.
     * The SliceFactory should produce lazy slices.
     */
    @Test
    public void LazySliceTestContextAware() {
        windowManager.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 1000));
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        assertTrue(windowManager.getMaxLateness() > 0); // out-of-order stream
        assertTrue(windowManager.hasContextAwareWindow()); // no context free window
        assertFalse(windowManager.isSessionWindowCase()); // no special case of session window, because of other context aware window

        Slice<Integer, Integer> slice = sliceFactory.createSlice(0, 10,   new Slice.Fixed());

        assertTrue("Slice factory produced Eager Slice", slice instanceof LazySlice);
    }

    public class TestWindow implements ForwardContextAware{

        WindowMeasure windowMeasure;

        public TestWindow(WindowMeasure windowMeasure) {
            this.windowMeasure = windowMeasure;
        }

        @Override
        public WindowContext createContext() {
            return null;
        }

        @Override
        public WindowMeasure getWindowMeasure() {
            return windowMeasure;
        }

        @Override
        public boolean isOverlapping() {
            return true;
        }
    }

}
