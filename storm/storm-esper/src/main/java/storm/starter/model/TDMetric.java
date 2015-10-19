package storm.starter.model;

import storm.starter.util.WindowConstant;

/**
 * Created by zhengqh on 15/9/21.
 */
public class TDMetric {

    private String partnerCode;
    private String appName;
    private String eventType;
    private String masterField;
    private String slaveField;
    private Integer timeUnit;
    private Compute compute;

    private String masterValue;
    private String slaveValue;

    public TDMetric(String partnerCode, String appName, String eventType, String masterField, String slaveField, Integer timeUnit, Compute compute) {
        this.partnerCode = partnerCode;
        this.appName = appName;
        this.eventType = eventType;
        this.masterField = masterField;
        this.slaveField = slaveField;
        this.timeUnit = timeUnit;
        this.compute = compute;
    }

    public String getPartnerCode() {
        return partnerCode;
    }

    public void setPartnerCode(String partnerCode) {
        this.partnerCode = partnerCode;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getMasterField() {
        return masterField;
    }

    public void setMasterField(String masterField) {
        this.masterField = masterField;
    }

    public String getSlaveField() {
        return slaveField;
    }

    public void setSlaveField(String slaveField) {
        this.slaveField = slaveField;
    }

    public Integer getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(Integer timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Compute getCompute() {
        return compute;
    }

    public void setCompute(Compute compute) {
        this.compute = compute;
    }

    public String getMasterValue() {
        return masterValue;
    }

    public void setMasterValue(String masterValue) {
        this.masterValue = masterValue;
    }

    public String getSlaveValue() {
        return slaveValue;
    }

    public void setSlaveValue(String slaveValue) {
        this.slaveValue = slaveValue;
    }

    public String getKey(){
        return partnerCode + WindowConstant.splitKey + appName + WindowConstant.splitKey + eventType + WindowConstant.splitKey;
    }

    @Override
    public String toString() {
        return "TDMetric{" +
                "partnerCode='" + partnerCode + '\'' +
                ", appName='" + appName + '\'' +
                ", eventType='" + eventType + '\'' +
                ", masterField='" + masterField + '\'' +
                ", slaveField='" + slaveField + '\'' +
                ", timeUnit=" + timeUnit +
                ", compute=" + compute +
                ", masterValue='" + masterValue + '\'' +
                ", slaveValue='" + slaveValue + '\'' +
                '}';
    }
}