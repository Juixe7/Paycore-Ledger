package com.paycore.payments.merchant;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MerchantController {

    private final MerchantService merchantService;

    public MerchantController(MerchantService merchantService) {
        this.merchantService = merchantService;
    }

    @PostMapping("/v1/merchants")
    public ResponseEntity<MerchantService.CreatedMerchantResponse> createMerchant(
            @RequestBody MerchantService.CreateMerchantRequest request) {
        MerchantService.CreatedMerchantResponse created = merchantService.createMerchant(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }
}
