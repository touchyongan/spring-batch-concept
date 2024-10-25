package io.kmaker.batch.dynamic;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.util.Assert;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DynamicPreparedStmtSetter implements ItemPreparedStatementSetter<Map<String, Object>> {
    private final List<String> columns;

    public DynamicPreparedStmtSetter(List<String> columns) {
        Assert.notEmpty(columns, "columns not provide for prepare statement");
        this.columns = columns;
    }

    @Override
    public void setValues(Map<String, Object> item,
                          PreparedStatement ps) throws SQLException {
        int idx = 1;
        for (String colum : columns) {
            ps.setObject(idx++, item.get(colum));
        }
    }
}
