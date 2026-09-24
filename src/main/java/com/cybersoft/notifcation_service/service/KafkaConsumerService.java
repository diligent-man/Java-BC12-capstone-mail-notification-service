package com.cybersoft.notifcation_service.service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @Autowired
    private JavaMailSender mailSender;

    @Value("${spring.mail.username}")
    private String serverEmail;

    // Lắng nghe hòm thư "user-registration-topic"
    @KafkaListener(topics = "user_registration_email", groupId = "group-email")
    public void listenRegistrationEvent(String userEmail) {
        System.out.println("[Consumer] Đang chuẩn bị gửi mail cho: " + userEmail);

        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(serverEmail);
            message.setTo(userEmail);
            message.setSubject("Đăng ký tài khoản thành công");
            message.setText("Chào mừng bạn đến với hệ thống của chúng tôi. Chúc bạn một ngày tốt lành");

            mailSender.send(message);
            System.out.println("[Consumer] Gửi thành công tới: " + userEmail);

        } catch (Exception e) {
            System.err.println("[Consumer] Lỗi khi gửi mail: " + e.getMessage());
        }
    }

    // Lắng nghe hòm thư "order.payment" — nhận payload JSON từ OutboxPublisherJob
    @KafkaListener(topics = "order.payment", groupId = "group-email")
    public void listenOrderPaymentEvent(String payloadJson) {
        System.out.println("[Consumer] Nhận được message từ topic order.payment");

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
                itemsText.append(String.format("  - %s | Màu: %s | Size: %s | SL: %d | Giá: %s USD\n",
                        item.get("productName").asText(),
                        item.get("color").asText(),
                        item.get("size").asText(),
                        item.get("quantity").asInt(),
                        item.get("unitPrice").asText()
                ));
            }

            // 4. Build nội dung email
            String emailBody = String.format(
                    "Xin chào %s,\n\n" +
                            "Đơn hàng của bạn đã được thanh toán thành công!\n\n" +
                            "--- THÔNG TIN ĐƠN HÀNG ---\n" +
                            "Mã đơn: #%d\n" +
                            "Ngày đặt: %s\n" +
                            "Phương thức thanh toán: %s\n\n" +
                            "--- SẢN PHẨM ---\n%s\n" +
                            "Tổng tiền: %s USD\n\n" +
                            "--- GIAO HÀNG ---\n" +
                            "Địa chỉ: %s, %s, %s\n" +
                            "Số điện thoại: %s\n" +
                            "Dự kiến giao: %s\n\n" +
                            "Cảm ơn bạn đã mua hàng tại UniClub!\n",
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
            System.out.println("[Consumer] Gửi email xác nhận đơn hàng #" + orderId + " tới: " + customerEmail);

        } catch (Exception e) {
            System.err.println("[Consumer] Lỗi khi xử lý message order.payment: " + e.getMessage());
            e.printStackTrace();
        }
    }

}