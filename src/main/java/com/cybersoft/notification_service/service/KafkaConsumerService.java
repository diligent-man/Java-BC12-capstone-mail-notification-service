package com.cybersoft.notification_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;


@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaConsumerService {
    private final JavaMailSender mailSender;

    @Value("${spring.mail.username}")
    private String serverEmail;


    // Lắng nghe hòm thư "user-registration-topic"
    @KafkaListener(topics = "user-registration-email", groupId = "group-email")
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
}
