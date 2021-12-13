package de.tub.dima.scotty.core.windowFunction;


public interface ReduceAggregateFunction<InputType> extends AggregateFunction<InputType, InputType, InputType> {

    @Override
    default InputType lift(InputType inputTuple) {
        return inputTuple;
    }

    @Override
    default InputType lower(InputType aggregate) {
        return aggregate;
    }
}
