CREATE DATABASE IF NOT EXISTS ordersdb;

USE ordersdb;

CREATE TABLE IF NOT EXISTS orders(
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(100),
    product VARCHAR(100),
    amount DECIMAL(10,2),
    status VARCHAR(30)
);

