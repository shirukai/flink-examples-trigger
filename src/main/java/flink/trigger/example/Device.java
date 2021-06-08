package flink.trigger.example;

import java.io.Serializable;

/**
 * POJO
 *
 * @author shirukai
 */
public class Device implements Serializable {
    private static final long serialVersionUID = -5101601835773326659L;
    /**
     * 设备ID
     */
    private String id;
    /**
     * 设备值
     */
    private Double value;
    /**
     * 设备时间戳
     */
    private Long timestamp;

    public Device() {
    }

    public Device(String id, Double value, Long timestamp) {
        this.id = id;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Device{" +
                "id='" + id + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
