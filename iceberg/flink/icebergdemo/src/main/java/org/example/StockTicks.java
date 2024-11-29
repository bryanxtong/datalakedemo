package org.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StockTicks {

    private long volume;
    private String ts;
    private String symbol;
    private int year;
    private String month;
    private double high;
    private double low;
    private String key;
    private String date;
    private double close;
    private double open;
    private String day;

    public StockTicks() {
    }

    public StockTicks(long volume, String ts, String symbol, int year, String month, double high, double low, String key, String date, double close, double open, String day) {
        this.volume = volume;
        this.ts = ts;
        this.symbol = symbol;
        this.year = year;
        this.month = month;
        this.high = high;
        this.low = low;
        this.key = key;
        this.date = date;
        this.close = close;
        this.open = open;
        this.day = day;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return "StockTicks{" +
                "volume=" + volume +
                ", ts='" + ts + '\'' +
                ", symbol='" + symbol + '\'' +
                ", year=" + year +
                ", month='" + month + '\'' +
                ", high=" + high +
                ", low=" + low +
                ", key='" + key + '\'' +
                ", date='" + date + '\'' +
                ", close=" + close +
                ", open=" + open +
                ", day='" + day + '\'' +
                '}';
    }
}
