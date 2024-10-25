package io.kmaker.batch.dynamic;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.util.Assert;
import org.springframework.validation.BindException;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DynamicFieldMapSet implements FieldSetMapper<Map<String, Object>> {

    private final List<String> columns;

    public DynamicFieldMapSet(String columns) {
        Assert.hasText(columns, "columns not provide for field mapper");
        this.columns = Arrays.stream(columns.split(","))
                .map(String::strip)
                .toList();
    }

    @Override
    public Map<String, Object> mapFieldSet(FieldSet fieldSet) throws BindException {
        Map<String, Object> data = new LinkedHashMap<>();
        for (String column : columns) {
            data.put(column, fieldSet.readString(column));
        }
        return data;
    }
}
