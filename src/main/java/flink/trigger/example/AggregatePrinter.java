package flink.trigger.example;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 聚合结果打印
 *
 * @author shirukai
 */
public class AggregatePrinter extends ProcessWindowFunction<Device, String, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<Device> elements, Collector<String> out) throws Exception {
        String result = StreamSupport.stream(elements.spliterator(), false).collect(Collectors.toList()).toString();
        out.collect(result);
    }
}
