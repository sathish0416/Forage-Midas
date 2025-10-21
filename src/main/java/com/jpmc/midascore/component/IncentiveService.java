package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";
    
    private final RestTemplate restTemplate;
    
    public IncentiveService(RestTemplateBuilder restTemplateBuilder) {
        this.restTemplate = restTemplateBuilder.build();
    }
    
    /**
     * Calls the external Incentive API to get the incentive amount for a transaction
     * @param transaction The transaction to calculate incentive for
     * @return Incentive object containing the incentive amount (>= 0)
     */
    public Incentive getIncentive(Transaction transaction) {
        try {
            // POST the transaction to the incentive API
            Incentive incentive = restTemplate.postForObject(
                INCENTIVE_API_URL, 
                transaction, 
                Incentive.class
            );
            
            logger.debug("Incentive received for transaction: {}", incentive);
            return incentive;
            
        } catch (Exception e) {
            logger.error("Failed to get incentive from API: {}", e.getMessage());
            // Return zero incentive if API call fails
            return new Incentive(0);
        }
    }
}
