package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private int transactionCount = 0;
    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;
    
    public TransactionListener(DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
    }
    
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void receiveTransaction(Transaction transaction) {
        
        transactionCount++;
        
        // Step 1: Validate sender exists
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Transaction rejected: Invalid sender ID {}", transaction.getSenderId());
            return;
        }
        
        // Step 2: Validate recipient exists
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Transaction rejected: Invalid recipient ID {}", transaction.getRecipientId());
            return;
        }
        
        // Step 3: Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Transaction rejected: Insufficient balance. Sender {} has {}, needs {}",
                    sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }
        
        // All validations passed - process the transaction
        
        // Step 4: Get incentive from the Incentive API
        Incentive incentive = incentiveService.getIncentive(transaction);
        float incentiveAmount = incentive.getAmount();
        
        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        // Recipient gets transaction amount PLUS incentive (bonus money!)
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        
        // Save updated user records
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        
        // Create and save transaction record with incentive
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        databaseConduit.saveTransaction(transactionRecord);
        
        if (transactionCount <= 4) {
            logger.info("========================================");
            logger.info("TRANSACTION #{}: AMOUNT = {} | INCENTIVE = {}", transactionCount, transaction.getAmount(), incentiveAmount);
            logger.info("Processed: {} -> {} (${} + ${} incentive) | Sender balance: {} | Recipient balance: {}", 
                    sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount,
                    sender.getBalance(), recipient.getBalance());
            logger.info("========================================");
        }
    }
}
