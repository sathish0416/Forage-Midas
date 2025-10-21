package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFourQuickTest {
    static final Logger logger = LoggerFactory.getLogger(TaskFourQuickTest.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private DatabaseConduit databaseConduit;

    @Test
    void find_wilbur_balance() throws InterruptedException {
        // Populate users
        userPopulator.populate();
        
        // Send all transactions
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        // Wait for processing (give extra time for API calls)
        Thread.sleep(3000);

        // Query wilbur's balance
        UserRecord wilbur = databaseConduit.findUserByName("wilbur");

        // Print the answer
        System.out.println("=".repeat(80));
        System.out.println("=".repeat(80));
        System.out.println("WILBUR'S BALANCE: " + wilbur.getBalance());
        System.out.println("WILBUR'S BALANCE (ROUNDED DOWN): " + (int) wilbur.getBalance());
        System.out.println("=".repeat(80));
        System.out.println("=".repeat(80));
        
        logger.info("=".repeat(80));
        logger.info("WILBUR'S BALANCE: {}", wilbur.getBalance());
        logger.info("WILBUR'S BALANCE (ROUNDED DOWN): {}", (int) wilbur.getBalance());
        logger.info("=".repeat(80));
    }
}
