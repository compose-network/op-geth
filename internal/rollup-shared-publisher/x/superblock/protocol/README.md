# Superblock Protocol Module

This package hands the interface for SBCP, with messages with callbacks and validation functions.

## Documentation

```mermaid
classDiagram
    direction LR

    class handler {
      -messageHandler: MessageHandler
      -validator: Validator
      +Handle(ctx, from, msg) error
      +CanHandle(msg) bool
      +GetProtocolName() string
    }

    class MessageHandler {
    <<interface>>
      +HandleStartSlot(ctx, from, StartSlot) error
      +HandleRequestSeal(ctx, from, RequestSeal) error
      +HandleL2Block(ctx, from, L2Block) error
      +HandleStartSC(ctx, from, StartSC) error
      +HandleRollBackAndStartSlot(ctx, from, RollBackAndStartSlot) error
    }

    class Validator {
      +ValidateStartSlot(StartSlot) error
      +ValidateRequestSeal(RequestSeal) error
      +ValidateL2Block(L2Block) error
      +ValidateStartSC(StartSC) error
      +ValidateRollBackAndStartSlot(RollBackAndStartSlot) error
    }
    
    class MessageType {
      <<enum>>
      MsgStartSlot
      MsgRequestSeal
      MsgL2Block
      MsgStartSC
      MsgRollBackAndStartSlot
    }


    handler --> MessageHandler : Processes with
    handler --> Validator : Validates with
    MessageType --> handler : Msgs sent to

```

### MessageType (types.go)
Enum describing of the SBCP message types (start slot, request seal, L2 block submission, start SC, and rollback).

### Validator (interface and `basicValidator` implementation)
Validation functions of SBCP messages.

### MessageHandler (interface)
Holds callbacks for each SBCP message.

### Handler (interface and `handler` implementation)
Holds a validator and a message handler. Receives messages by validating and then processing them.
