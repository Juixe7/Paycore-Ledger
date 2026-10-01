package com.paycore.mockpsp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MockPspControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void authorizesIdempotentlyByPspRefAndExportsSettlementCsvWithDiscrepancies() {
        MockPspController.AuthRequest req1 =
                new MockPspController.AuthRequest("pay-1", "merch-1", "pay-1:1", 1000L, "INR");
        MockPspController.AuthRequest req2 =
                new MockPspController.AuthRequest("pay-2", "merch-1", "pay-2:1", 2000L, "INR");

        ResponseEntity<MockPspController.AuthResponse> resp1 =
                restTemplate.postForEntity("/psp/v1/authorizations", req1, MockPspController.AuthResponse.class);
        ResponseEntity<MockPspController.AuthResponse> resp2 =
                restTemplate.postForEntity("/psp/v1/authorizations", req2, MockPspController.AuthResponse.class);

        assertThat(resp1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp1.getBody().status()).isEqualTo("AUTHORIZED");
        assertThat(resp2.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Lookup by pspRef returns AUTHORIZED
        ResponseEntity<MockPspController.LookupResponse> lookup =
                restTemplate.getForEntity("/psp/v1/authorizations/pay-1:1", MockPspController.LookupResponse.class);
        assertThat(lookup.getBody().status()).isEqualTo("AUTHORIZED");
        assertThat(lookup.getBody().amountMinor()).isEqualTo(1000L);

        // Capture and partial refund on PSP
        ResponseEntity<MockPspController.AuthResponse> capResp = restTemplate.postForEntity(
                "/psp/v1/captures/pay-1:1",
                new MockPspController.CaptureRequest("pay-1", 1000L, "INR"),
                MockPspController.AuthResponse.class);
        assertThat(capResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(capResp.getBody().status()).isEqualTo("CAPTURED");

        ResponseEntity<MockPspController.AuthResponse> refResp = restTemplate.postForEntity(
                "/psp/v1/refunds/pay-1:1",
                new MockPspController.RefundPspRequest("pay-1", 300L, "INR"),
                MockPspController.AuthResponse.class);
        assertThat(refResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(refResp.getBody().amountMinor()).isEqualTo(700L);

        // Settlement CSV with injected discrepancies
        ResponseEntity<String> csv =
                restTemplate.getForEntity("/psp/v1/settlement.csv?injectDiscrepancies=true", String.class);
        assertThat(csv.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(csv.getBody()).contains("psp_ref,amount,status,settled_at");
        assertThat(csv.getBody()).contains("phantom-psp-ref:1");
    }
}
