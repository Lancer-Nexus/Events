# Lancer Nexus Events

The Events repository defines large-scale event and battle workflows for Lancer Nexus.

## Responsibilities

- Event definitions and schedules
- Registration and eligibility
- Reserved event-server lifecycle
- Participant and group admission
- Event start, lock, result and return-transfer states
- Event-specific metrics and audit records

Event logic integrates with the Gateway and Coordinator but does not replace the authoritative game-server simulation.

## Shared Protocol

The shared contracts are checked out in the `Protocol` submodule. Update it before local builds with:

```bash
git submodule update --init --remote --merge Protocol
```

CI performs the same update before restoring and building Events.
