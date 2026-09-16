package com.example.daprDemo.daprDemo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jdk.jfr.DataAmount;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private Long id;
    @JsonProperty("customer_name")
    private String customer_name;
    private String product;
    private BigDecimal amount;
    private String status;


}