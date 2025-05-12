# go-events

A Golang application that demonstrates a clean, layered architecture for a Kafka-based event system.

## Project Structure

The project follows a layered architecture with clear separation of concerns:

```
/internal
  /domain             # Core business logic and entities
    /model            # Domain models/entities
    /repository       # Repository interfaces
    /service          # Business logic
  /infrastructure     # Implementation details
    /api              # HTTP API handlers and routing
    /kafka            # Kafka consumers and producers
    /persistence      # Database implementation
```

### Layers

1. **Domain Layer**
   - Contains the core business logic
   - Defines domain models and interfaces
   - Independent of infrastructure concerns

2. **Service Layer**
   - Implements business logic
   - Uses domain entities and repository interfaces
   - Serves as an abstraction between domain and infrastructure

3. **Infrastructure Layer**
   - Implements the repository interfaces
   - Contains framework-specific code (Kafka, GORM, Gin)
   - Provides concrete implementations for the abstractions defined in the domain layer

## Running the Application

```bash
# Start the required infrastructure
docker-compose up -d

# Build and run the application
go build
./go-events
```

## API Endpoints

- `GET /ping` - Sends a message to Kafka and returns "pong"
- `GET /hello` - Returns a simple hello message

## Design Principles

- **Dependency Inversion**: Upper layers define interfaces that lower layers implement
- **Single Responsibility**: Each component handles one specific concern
- **Separation of Concerns**: Clear boundaries between different parts of the application
- **Domain-Driven Design**: Core business logic is independent of infrastructure details