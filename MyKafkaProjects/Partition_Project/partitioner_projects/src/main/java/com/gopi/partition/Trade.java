package com.gopi.partition;

public class Trade {
    private String id;
    private String securityId;
    private String fundShortName;
    private String value;

    /**public Trade(String id, String securityId, String fundShortName, String value) {
        this.id= id;
        this.securityId= securityId;
        this.fundShortName= fundShortName;
        this.value= value;
    }
    */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSecurityId() {
        return securityId;
    }

    public void setSecurityId(String securityId) {
        this.securityId = securityId;
    }

    public String getFundShortName() {
        return fundShortName;
    }

    public void setFundShortName(String fundShortName) {
        this.fundShortName = fundShortName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}