package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.FixedBandWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class FixedBandWindowTest {

    private SlicingWindowOperator<Integer> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
    }

    @Test
    public void inOrderTest() {
        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 1, 10));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0),1,11,1);
    }

    @Test
    public void inOrderTest2() {
        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 0, 10));
        slicingWindowOperator.processElement(1, 0);
        slicingWindowOperator.processElement(2, 0);
        slicingWindowOperator.processElement(3, 20);
        slicingWindowOperator.processElement(4, 30);
        slicingWindowOperator.processElement(5, 40);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        WindowAssert.assertEquals(resultWindows.get(0),0,10,3);

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertTrue(resultWindows.isEmpty());
    }

    @Test
    public void inOrderTest3() {
        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 18, 10));
        slicingWindowOperator.processElement(1, 0);
        slicingWindowOperator.processElement(2, 0);
        slicingWindowOperator.processElement(3, 20);
        slicingWindowOperator.processElement(4, 30);
        slicingWindowOperator.processElement(5, 40);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        Assert.assertTrue(resultWindows.isEmpty());

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0),18,28,3);
    }


    @Test
    public void inOrderTwoWindowsTest() {
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 10, 10));
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 20, 10));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        Assert.assertEquals(2, resultWindows.get(0).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));

    }

    @Test
    public void inOrderTwoWindowsTest2() {
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 14, 11));
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 23, 10));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(26);
        Assert.assertEquals(2, resultWindows.get(0).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));

    }

    @Test
    public void inOrderTwoWindowsDynamicTest() {

        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 10, 10));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 20, 10));
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        Assert.assertEquals(2, resultWindows.get(0).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));

    }

    @Test
    public void inOrderTwoWindowsDynamicTest2() {
        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 10, 10));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        Assert.assertEquals(2, resultWindows.get(0).getAggValues().get(0));

        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 20, 21));
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(7, resultWindows.get(0).getAggValues().get(0));
    }


    @Test
    public void outOfOrderTest() {
        slicingWindowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer currentAggregate, Integer element) {
                return currentAggregate + element;
            }

            @Override
            public Integer invert(Integer currentAggregate, Integer element) {
                return currentAggregate - element;
            }
        });
        slicingWindowOperator.addWindowAssigner(new FixedBandWindow(WindowMeasure.Time, 10, 20));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 29);

        //out-of-order tuples have to be inserted into the window
        slicingWindowOperator.processElement(1, 20);
        slicingWindowOperator.processElement(1, 23);
        slicingWindowOperator.processElement(1, 25);

        slicingWindowOperator.processElement(1, 45);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        Assert.assertTrue(resultWindows.isEmpty());

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(4, resultWindows.get(0).getAggValues().get(0));

    }

}
