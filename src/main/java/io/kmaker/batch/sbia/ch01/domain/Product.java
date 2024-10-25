package io.kmaker.batch.sbia.ch01.domain;

import lombok.Getter;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;

@Getter
@Setter
public class Product implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String description;
    private BigDecimal price;

    public Product(String id) {
        this.id = id;
    }

    public Product() {
    }

    @Override
    public String toString() {
        return id + "," + name + "," + description + "," + price;
    }
}
