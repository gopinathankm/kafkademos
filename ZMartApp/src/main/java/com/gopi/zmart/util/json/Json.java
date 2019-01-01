package com.gopi.zmart.util.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 *  Author: Gopinathan Munappy
 *  Date : 16/11/2018
 *  Time : 11.45 AM
 *
 */

public class Json {

    private JsonObject jsonObject;

    public Json(JsonObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public String propertyStringValue(String propertyName) {
        return propertyStringValueFunc(propertyName).apply(jsonObject);
    }

    public Long propertyLongValue(String propertyName) {
        return propertyLongValueFunc(propertyName).apply(jsonObject);
    }


    public static Function<JsonObject, String> propertyStringValueFunc(String name) {
        return jsonObject -> {
            JsonElement propertyValue = jsonObject.get(name);
            if(propertyValue != null) {
                return propertyValue.getAsString();
            }

            return null;
        };
    }

    public static Function<JsonObject, Long> propertyLongValueFunc(String name) {
        return jsonObject -> {
            JsonElement propertyValue = jsonObject.get(name);
            if(propertyValue != null) {
                return propertyValue.getAsLong();
            }

            return null;
        };
    }

    public static Function<JsonObject, Boolean> checkPropertyFunc(String propertyName, Predicate<String> condition) {
        return jsonObject -> {
            JsonElement jsonElement = jsonObject.get(propertyName);
            if(jsonElement != null) {
                return condition.test(jsonElement.getAsString());
            }

            return Boolean.FALSE;
        };
    }


}