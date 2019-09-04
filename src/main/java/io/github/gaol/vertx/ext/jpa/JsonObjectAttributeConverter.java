package io.github.gaol.vertx.ext.jpa;

import javax.persistence.AttributeConverter;

import io.vertx.core.json.JsonObject;

public class JsonObjectAttributeConverter implements AttributeConverter<JsonObject, String>{

    @Override
    public String convertToDatabaseColumn(JsonObject json) {
        return json.encode();
    }

    @Override
    public JsonObject convertToEntityAttribute(String dbData) {
        return new JsonObject(dbData);
    }

    
}
