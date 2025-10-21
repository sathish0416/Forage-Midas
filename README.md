
# 🏦 Midas Core – Financial Transaction Processing System

Project Repo for the JPMC Advanced Software Engineering Forage Program

## 📘 Overview
Midas Core is a Spring Boot–based system that processes real-time financial transactions using Kafka, stores data in an H2 in-memory database, integrates an Incentive API, and exposes a REST API for balance queries.

## ⚙️ Tech Stack
- Backend: Spring Boot (Java 17)
- Messaging: Apache Kafka
- Database: H2 (via JPA/Hibernate)
- API Client: RestTemplate
- Build Tool: Maven

## 🧩 Core Components
- Kafka Consumer: Handles incoming transactions
- Transaction Validator: Checks user IDs and balances
- Database Layer: Stores users and transactions
- Incentive Service: Calls external Incentive API
- Balance API: Returns current user balances

## ✅ Completed Tasks
### Task 3 – H2 Database Integration
- Added TransactionRecord and UserRecord JPA entities
- Implemented TransactionRepository for CRUD operations
- Validated and stored transactions with balance updates

### Task 4 – Incentive API Integration
- Created IncentiveService using RestTemplate
- Added incentive logic in TransactionListener
- Updated recipient balance with incentive amount

### Task 5 – Balance Query API
- Exposed /balance?userId={id} endpoint
- Returns user’s current balance as JSON
- Runs alongside Kafka consumer

## ▶️ Running the Project
git clone https://github.com/sathish0416/Forage-Midas.git
cd Forage-Midas
./mvnw clean install
./mvnw spring-boot:run

## 🌐 API
GET /balance?userId={userId}
Response:
{ "amount": 1234.56 }

## 🗄️ Database Schema
UserRecord: id, name, balance
TransactionRecord: id, sender_id, recipient_id, amount, incentive

## 📁 Key Files
src/
├── component/
│   ├── TransactionListener.java
│   ├── IncentiveService.java
│   └── BalanceController.java
├── entity/
│   ├── TransactionRecord.java
│   └── UserRecord.java
├── repository/
│   ├── TransactionRepository.java
│   └── UserRepository.java
└── foundation/
    ├── Transaction.java
    ├── Incentive.java
    └── Balance.java

## 💡 Learnings
- Using JPA for entity relationships
- Building REST APIs with Spring Boot
- Integrating external APIs
- Real-time processing with Kafka

## 👨‍💻 Author
Sathish Madanu
GitHub: https://github.com/sathish0416
## 🙏 Acknowledgments 
- JPMorgan Chase & Co. 
- Forage Virtual Experience Program
- Spring Boot Community make