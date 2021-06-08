package flink.trigger.example;


import org.apache.flink.api.common.functions.MapFunction;

/**
 * "device-1,1.0,1622701780701" -> Object: Device
 * @author shirukai
 */
public class DeviceTransformer implements MapFunction<String, Device> {
    @Override
    public Device map(String s) throws Exception {
        String[] items = s.split(",");
        return new Device(items[0], Double.parseDouble(items[1]), Long.parseLong(items[2]));
    }
}
