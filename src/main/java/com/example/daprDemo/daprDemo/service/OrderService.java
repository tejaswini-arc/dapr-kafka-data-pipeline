package com.example.daprDemo.daprDemo.service;

import com.example.daprDemo.daprDemo.model.Order;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

@Service
public class OrderService {

    private final RestClient restClient;

    public OrderService() {

        this.restClient = RestClient.builder()
                .baseUrl("http://order-app-dapr:3500")
                .build();
    }

    public void publishOrder(Order order) {

        System.out.println();
        System.out.println("========== PRODUCER ==========");
        System.out.println("Publishing order to Dapr...");
        System.out.println("Order ID      : " + order.getId());
        System.out.println("Customer Name : " + order.getCustomer_name());
        System.out.println("Product       : " + order.getProduct());
        System.out.println("Amount        : " + order.getAmount());
        System.out.println("Status        : " + order.getStatus());

        restClient.post()
                .uri("/v1.0/publish/pubsub/orders")
                .body(order)
                .retrieve()
                .toBodilessEntity();

        System.out.println("[PRODUCER] Order published successfully");
        System.out.println("==============================");
    }
}