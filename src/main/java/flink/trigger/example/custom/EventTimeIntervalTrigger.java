package flink.trigger.example.custom;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Objects;

/**
 * 事件时间间隔触发器
 * copy from {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger}
 * 重写 {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger#onEventTime(long, TimeWindow, TriggerContext)}
 * 和 {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger#onProcessingTime(long, TimeWindow, TriggerContext)}
 * 方法：
 * 1. 在onElement方法里增加基于处理时间的定时器
 * 2. 在onProcessingTime方法里增加定时器触发后将窗口发出的逻辑
 *
 * @author shirukai
 */
public class EventTimeIntervalTrigger<T> extends Trigger<T, TimeWindow> {
    private final long interval;

    /**
     * When merging we take the lowest of all fire timestamps as the new fire timestamp.
     */
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time", new EventTimeIntervalTrigger.Min(), LongSerializer.INSTANCE);

    private EventTimeIntervalTrigger(long interval) {
        this.interval = interval;
    }

    /**
     * 创建trigger实例
     *
     * @param interval 间隔
     * @param <T>      数据类型泛型
     * @return EventTimeIntervalTrigger
     */
    public static <T> EventTimeIntervalTrigger<T> of(Time interval) {
        return new EventTimeIntervalTrigger<>(interval.toMilliseconds());
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // 判断定时器是否注册过
            ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
            if (Objects.isNull(fireTimestamp.get()) || fireTimestamp.get() < 0) {
                // 注册一个基于处理时间的计时器
                long currentTimestamp = ctx.getCurrentProcessingTime();
                long nextFireTimestamp = currentTimestamp - (currentTimestamp % interval) + interval;
                ctx.registerProcessingTimeTimer(nextFireTimestamp);
                fireTimestamp.add(nextFireTimestamp);
            }
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).add(Long.MIN_VALUE);
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return time == window.maxTimestamp() ?
                TriggerResult.FIRE :
                TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        // 清理事件时间定时器
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        // 清理处理时间定时器
        if (Objects.isNull(fireTimestamp.get()) || fireTimestamp.get() > 0) {
            ctx.deleteProcessingTimeTimer(fireTimestamp.get());
        }
        // 清理状态
        fireTimestamp.clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    @Override
    public String toString() {
        return "EventTimeIntervalTrigger()";
    }
}
