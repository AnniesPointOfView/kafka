package com.hb.kafka.producer.controller;

import com.hb.kafka.producer.model.*;
import com.hb.kafka.producer.service.*;
import lombok.*;
import lombok.experimental.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.HttpStatus.*;
import static org.springframework.http.ResponseEntity.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/message")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SenderController {

    MessageSender messageSender;

    @PostMapping("/send")
    public ResponseEntity<String> send(@RequestParam(value = "part", required = false) Integer partition,
                                       @RequestParam(value = "topic") String topicName,
                                       @RequestBody Message message) {
        if (messageSender.send(message.getMessageText(), topicName, partition)) {
            return ResponseEntity.ok("ok");
        }
        return status(INTERNAL_SERVER_ERROR)
                .body("kafka isn't available");
    }

}
