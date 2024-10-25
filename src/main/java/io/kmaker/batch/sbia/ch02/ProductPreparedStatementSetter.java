package io.kmaker.batch.sbia.ch02;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ProductPreparedStatementSetter implements ItemPreparedStatementSetter<Product> {

    @Override
    public void setValues(Product product,
                          PreparedStatement ps) throws SQLException {
        ps.setString(1, product.getId());
        ps.setString(2, product.getName());
        ps.setString(3, product.getDescription());
        ps.setFloat(4, product.getPrice());
    }
}
