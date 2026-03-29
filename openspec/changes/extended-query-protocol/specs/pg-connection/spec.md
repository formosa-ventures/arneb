## MODIFIED Requirements

### Requirement: Connection handler factory provides query handlers
The HandlerFactory SHALL provide a fully functional `ExtendedQueryHandler` implementation instead of `PlaceholderExtendedQueryHandler`. The SimpleQueryHandler SHALL remain unchanged.

#### Scenario: Factory returns extended query handler
- **WHEN** a new connection is established
- **THEN** HandlerFactory returns an ExtendedQueryHandler that supports Parse, Bind, Describe, Execute, Sync, and Close

#### Scenario: Simple query still works
- **WHEN** a client sends a Simple Query message
- **THEN** the existing SimpleQueryHandler processes it as before (no regression)
