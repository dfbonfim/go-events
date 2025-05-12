# Architecture

The application follows a layered architecture with clear separation of concerns to ensure maintainability and testability.

```
┌─────────────────────────────────────────┐
│                                         │
│  Entrypoint (main.go)                   │
│                                         │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│                                         │
│  Infrastructure Layer                   │
│                                         │
│  ┌─────────────┐  ┌─────────────────┐   │
│  │             │  │                 │   │
│  │  API        │  │  Kafka          │   │
│  │  (Gin)      │  │  (Consumers/    │   │
│  │             │  │   Producers)    │   │
│  └──────┬──────┘  └────────┬────────┘   │
│         │                  │            │
│         ▼                  ▼            │
│  ┌─────────────────────────────────┐    │
│  │                                 │    │
│  │  Persistence                    │    │
│  │  (GORM)                         │    │
│  │                                 │    │
│  └─────────────────────────────────┘    │
│                                         │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│                                         │
│  Domain Layer                           │
│                                         │
│  ┌─────────────┐  ┌─────────────────┐   │
│  │             │  │                 │   │
│  │  Services   │  │  Repository     │   │
│  │             │  │  Interfaces     │   │
│  └──────┬──────┘  └────────┬────────┘   │
│         │                  │            │
│         └──────────┬───────┘            │
│                    │                    │
│                    ▼                    │
│  ┌─────────────────────────────────┐    │
│  │                                 │    │
│  │  Domain Models                  │    │
│  │                                 │    │
│  └─────────────────────────────────┘    │
│                                         │
└─────────────────────────────────────────┘
```

## Data Flow

1. **API Request Flow**:
   - API handler receives HTTP request
   - Handler calls appropriate domain service
   - Service uses repository interface to access data
   - Repository implementation handles data storage

2. **Kafka Consumer Flow**:
   - Kafka consumer receives message
   - Consumer calls domain service with message data
   - Service applies business logic
   - Service uses repository to persist data

3. **Kafka Producer Flow**:
   - API or service initiates message production
   - Producer sends message to Kafka topic
   - Delivery status is logged

## Dependencies

Dependencies flow inward, with the domain layer being the most independent:

```
Infrastructure → Domain
```

- Domain layer has no dependencies on infrastructure
- Infrastructure layer implements interfaces defined in domain
- External frameworks (Kafka, Gin, GORM) are isolated in the infrastructure layer