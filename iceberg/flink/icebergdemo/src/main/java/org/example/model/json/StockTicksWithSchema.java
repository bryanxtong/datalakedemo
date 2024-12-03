package org.example.model.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@io.confluent.kafka.schemaregistry.annotations.Schema(value = StockTicksWithSchema.SCHEMA_AS_STRING,
        refs = {})
@JsonIgnoreProperties(ignoreUnknown = true)
public class StockTicksWithSchema {
    public static final String SCHEMA_AS_STRING = """
               {
                  "schemaType": "JSON",
                  "$schema": "https://json-schema.org/draft/2020-12/schema",
                  "$id": "https://example.com/stockticks.schema.json",
                  "title": "StockTicks",
                  "description": "stock ticks",
                  "type": "object",
                  "properties": {
                    "volume": {
                      "type": "number"
                    },
                    "symbol": {
                      "type": "string"
                    },
                    "ts": {
                      "type": "string"
                    },
                    "month": {
                      "type": "string"
                    },
                    "high": {
                      "type": "number"
                    },
                    "low": {
                      "type": "number"
                    },
                    "key": {
                      "type": "string"
                    },
                    "year": {
                      "type": "integer"
                    },
                    "date": {
                      "type": "string"
                    },
                    "close": {
                      "type": "number"
                    },
                    "open": {
                      "type": "number"
                    },
                    "day": {
                      "type": "string"
                    }
                  }
                }
            """;

    private long volume;
    private String symbol;
    private String ts;
    private String month;
    private double high;
    private double low;
    private String key;
    private int year;
    private String date;
    private double close;
    private double open;
    private String day;

    public StockTicksWithSchema() {
    }

/*    public StockTicks(long volume, String symbol, String ts, String month, double high, double low, String key, int year, String date, double close, double open, String day) {
        this.volume = volume;
        this.symbol = symbol;
        this.ts = ts;
        this.month = month;
        this.high = high;
        this.low = low;
        this.key = key;
        this.year = year;
        this.date = date;
        this.close = close;
        this.open = open;
        this.day = day;
    }*/

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
        return "StockTicksWithSchema{" +
                "volume=" + volume +
                ", symbol='" + symbol + '\'' +
                ", ts='" + ts + '\'' +
                ", month='" + month + '\'' +
                ", high=" + high +
                ", low=" + low +
                ", key='" + key + '\'' +
                ", year=" + year +
                ", date='" + date + '\'' +
                ", close=" + close +
                ", open=" + open +
                ", day='" + day + '\'' +
                '}';
    }
}
