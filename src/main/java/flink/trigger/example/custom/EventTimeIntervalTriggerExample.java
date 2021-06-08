package flink.trigger.example.custom;

import flink.trigger.example.AggregatePrinter;
import flink.trigger.example.Device;
import flink.trigger.example.DeviceTransformer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

/**
 * 事件时间窗口示例
 * nc -lk 9000
 * device-1,1.0,1622708520000
 * device-1,1.0,1622708580000
 * device-1,1.0,1622708640000
 * @author shirukai
 */
public class EventTimeIntervalTriggerExample {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 时间语义设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3. 创建一个socket数据源
        DataStream<String> socketSource = env.socketTextStream("localhost", 9000);

        DataStream<String> deviceEventDataStream = socketSource
                // 4. 将数据源String类型数据格式化为DeviceEvent
                .map(new DeviceTransformer())
                // 5. 分发事件时间和水印
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Device>(Time.seconds(30)) {
                    @Override
                    public long extractTimestamp(Device element) {
                        return element.getTimestamp();
                    }
                })
                // 6. 按照id进行分组
                .keyBy("id")
                // 7. 设置1分钟的滚动窗口
                .timeWindow(Time.minutes(1))
                // 8. 设置触发器
                .trigger(EventTimeIntervalTrigger.of(Time.seconds(10)))
                // 9. 自定义聚合器
                .process(new AggregatePrinter());

        // 10. 结果输出控制台
        deviceEventDataStream.print("Print");

        // 11. 提交执行
        env.execute("EventTimeIntervalTriggerExample");

    }
}
