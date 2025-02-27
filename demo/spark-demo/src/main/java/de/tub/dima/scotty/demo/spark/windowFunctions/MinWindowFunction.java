package de.tub.dima.scotty.demo.spark.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;

public class MinWindowFunction implements ReduceAggregateFunction<Integer> {
    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        return Math.min(partialAggregate1, partialAggregate2);
    }

    @Override
    public Integer invert(Integer partialAggregate1, Integer partialAggregate2) {
        return null;
    }
}
