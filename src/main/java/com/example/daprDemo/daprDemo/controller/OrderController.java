package com.example.daprDemo.daprDemo.controller;

import com.example.daprDemo.daprDemo.model.Order;
import com.example.daprDemo.daprDemo.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody Order order) {

        orderService.publishOrder(order);

        return ResponseEntity.accepted().body(
                Map.of( "message", "Order published successfully",
                        "orderId", order.getId()
                )
        );
    }
}