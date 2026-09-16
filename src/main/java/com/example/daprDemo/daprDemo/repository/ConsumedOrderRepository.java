package com.example.daprDemo.daprDemo.repository;

import com.example.daprDemo.daprDemo.model.Order;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ConsumedOrderRepository {

        private final JdbcTemplate jdbcTemplate;

        public ConsumedOrderRepository(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        public void save(Order order) {

            String sql = """
                INSERT INTO orders
                (id, customer_name, product, amount, status)
                VALUES (?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    customer_name = VALUES(customer_name),
                    product = VALUES(product),
                    amount = VALUES(amount),
                    status = VALUES(status)
                """;

            jdbcTemplate.update(
                    sql,
                    order.getId(),
                    order.getCustomer_name(),
                    order.getProduct(),
                    order.getAmount(),
                    order.getStatus()
            );
        }
    }