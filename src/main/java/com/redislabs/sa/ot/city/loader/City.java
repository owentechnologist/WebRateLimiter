package com.redislabs.sa.ot.city.loader;

public class City {

    //"city","city_ascii","province_id","province_name","lat","lng","population","density","timezone","ranking","postal","id"
    String cityName;
    String provinceName;
    String provinceID;
    String lat;
    String lng;
    String id;
    String [] postalCodes;

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    public String getProvinceID() {
        return provinceID;
    }

    public void setProvinceID(String provinceID) {
        this.provinceID = provinceID;
    }

    public String getLng() {
        return lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String[] getPostalCodes() {
        return postalCodes;
    }

    public void setPostalCodes(String[] postalCodes) {
        this.postalCodes = postalCodes;
    }


    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }


}
