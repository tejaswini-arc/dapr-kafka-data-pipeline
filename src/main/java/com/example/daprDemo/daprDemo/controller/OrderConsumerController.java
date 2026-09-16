package com.example.daprDemo.daprDemo.controller;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import com.example.daprDemo.daprDemo.model.Order;
import com.example.daprDemo.daprDemo.repository.ConsumedOrderRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
@RestController
public class OrderConsumerController {


    private final ConsumedOrderRepository orderRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderConsumerController(ConsumedOrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }



    @PostMapping("/consume")
    public ResponseEntity<?> consumeOrder(
            @RequestBody JsonNode cloudEvent) throws Exception {

        System.out.println();
        System.out.println("========== CONSUMER ==========");
        System.out.println("[CONSUMER] CloudEvent received from Dapr");

        JsonNode data = cloudEvent.get("data");

        Order order = objectMapper.treeToValue(data, Order.class);

        System.out.println("Order ID      : " + order.getId());
        System.out.println("Customer Name : " + order.getCustomer_name());
        System.out.println("Product       : " + order.getProduct());
        System.out.println("Amount        : " + order.getAmount());
        System.out.println("Status        : " + order.getStatus());

        orderRepository.save(order);

        System.out.println("[CONSUMER] Order saved into MySQL");
        System.out.println("==============================");

        return ResponseEntity.ok(
                Map.of(
                        "message", "Order consumed and saved successfully",
                        "orderId", order.getId()
                )
        );
    }
}