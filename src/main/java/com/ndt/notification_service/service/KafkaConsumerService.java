package com.ndt.notification_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;


import org.springframework.stereotype.Service;

import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.beans.factory.annotation.Value;


@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final JavaMailSender mailSender;

    @Value("${spring.mail.username}")
    private String serverEmail;


    @KafkaListener(topics = "user_registration_email", groupId = "group-email")
    public void listenRegistrationEvent(String userEmail) {
        log.info("[Consumer] Đang chuẩn bị gửi mail cho: {}", userEmail);

        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(serverEmail);
            message.setTo(userEmail);
            message.setSubject("Đăng ký tài khoản thành công");
            message.setText("Chào mừng bạn đến với hệ thống của chúng tôi. Chúc bạn một ngày tốt lành");

            mailSender.send(message);
            log.info("[Consumer] Gửi thành công tới: {}", userEmail);

        } catch (Exception e) {
            log.error("[Consumer] Lỗi khi gửi mail: {}", e.getMessage());
        }
    }


    // Lắng nghe hòm thư "order.payment" — nhận payload JSON từ OutboxPublisherJob
    @KafkaListener(topics = "order.payment", groupId = "group-email")
    public void listenOrderPaymentEvent(String payloadJson) {
        log.info("[Consumer] Nhận được message từ topic order.payment");

        try {
            // 1. Parse chuỗi JSON thành object để đọc từng field
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(payloadJson);

            // 2. Đọc thông tin từ payload
            Long orderId = root.get("orderId").asLong();
            String customerName = root.get("customer").get("name").asText();
            String customerEmail = root.get("customer").get("email").asText();
            String customerPhone = root.get("customer").get("phone").asText();
            String customerAddress = root.get("customer").get("address").asText();
            String customerTown = root.get("customer").get("town").asText();
            String customerCountry = root.get("customer").get("country").asText();
            String totalAmount = root.get("totalAmount").asText();
            String orderDate = root.get("orderDate").asText();
            String estimatedDelivery = root.get("estimatedDelivery").asText();
            String paymentMethod = root.get("paymentMethod").asText();

            // 3. Đọc danh sách sản phẩm
            StringBuilder itemsText = new StringBuilder();
            JsonNode items = root.get("items");
            for (int i = 0; i < items.size(); i++) {
                JsonNode item = items.get(i);
                itemsText.append(
                    String.format("  - %s | Màu: %s | Size: %s | SL: %d | Giá: %s USD\n",
                        item.get("productName").asText(),
                        item.get("color").asText(),
                        item.get("size").asText(),
                        item.get("quantity").asInt(),
                        item.get("unitPrice").asText()
                    ));
            }

            // 4. Build nội dung email
            String emailBody = """
                Xin chào %s,
                
                Đơn hàng của bạn đã được thanh toán thành công!
                
                --- THÔNG TIN ĐƠN HÀNG ---
                Mã đơn: #%d
                Ngày đặt: %s
                Phương thức thanh toán: %s
                
                --- SẢN PHẨM ---
                %s
                Tổng tiền: %s USD
                
                --- GIAO HÀNG ---
                Địa chỉ: %s, %s, %s
                Số điện thoại: %s
                Dự kiến giao: %s
                
                Cảm ơn bạn đã mua hàng tại UniClub!
                """.formatted(
                customerName,
                orderId,
                orderDate,
                paymentMethod,
                itemsText.toString(),
                totalAmount,
                customerAddress,
                customerTown,
                customerCountry,
                customerPhone,
                estimatedDelivery
            );

            // 5. Gửi email
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(serverEmail);
            message.setTo(customerEmail);
            message.setSubject("UniClub - Xác nhận đơn hàng #" + orderId);
            message.setText(emailBody);

            mailSender.send(message);

            log.info("[Consumer] Gửi email xác nhận đơn hàng #{} tới: {}", orderId, customerEmail);
        } catch (Exception e) {
            log.error("[Consumer] Lỗi khi xử lý message order.payment: {}", e.getMessage(), e);
        }
    }
}
