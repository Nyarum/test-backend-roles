# inferno

inferno is a small internal component of our inference platform. It takes a prompt for a customer account, charges the account a flat number of credits from Postgres, forwards the prompt to a GPU node over HTTP, and streams the generated tokens back to the caller as they arrive. Token usage is metered per account in the background. The whole thing is a single Go package; the GPU node is simulated in tests.

## Setup

Requirements: Go 1.23+, Docker.

```sh
docker compose up -d
go test ./...
```

`go test -race ./...` is also allowed. Tests use `DATABASE_URL` if set, otherwise `postgres://inferno:inferno@localhost:5432/inferno?sslmode=disable`.

## Incidents

1. Enterprise customer reports that, during peak traffic, streamed responses occasionally contain text that clearly belongs to another customer's request. Cannot reproduce with single requests.

2. Finance reports that some customers consumed far more inference than the credits they purchased, especially during traffic spikes, even though every request checks the balance before charging.

3. Under sustained load, the GPU node team sees connection counts climbing steadily and eventually the gateway host logs `cannot assign requested address` and `too many open files`.

## Rules

- Fix the root causes, not the symptoms.
- Do not change the tests.
- Be ready to explain each fix and why it works.
