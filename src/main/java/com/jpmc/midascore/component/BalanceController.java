package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {
    
    private static final Logger logger = LoggerFactory.getLogger(BalanceController.class);
    private final DatabaseConduit databaseConduit;
    
    public BalanceController(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }
    
    /**
     * GET endpoint to query user balance by userId
     * @param userId The user ID to query
     * @return Balance object with the user's current balance (or 0 if user doesn't exist)
     */
    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        logger.debug("Balance query received for userId: {}", userId);
        
        // Query the user from database
        UserRecord user = databaseConduit.findUserById(userId);
        
        // If user doesn't exist, return balance of 0
        if (user == null) {
            logger.debug("User {} not found, returning balance of 0", userId);
            return new Balance(0);
        }
        
        // Return the user's current balance
        float userBalance = user.getBalance();
        logger.debug("User {} balance: {}", userId, userBalance);
        return new Balance(userBalance);
    }
}
