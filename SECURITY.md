# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Lux, please report it privately so we can fix it before it's exploited. **Please do not open a public GitHub issue** for security vulnerabilities, as this exposes the issue to everyone before a fix is available.

Email **[hello@pompeiilabs.com](mailto:hello@pompeiilabs.com)** with:

- A description of the vulnerability
- Steps to reproduce
- Affected versions (if known)
- Any potential impact assessment

## Response Timeline

We aim to acknowledge reports within a few business days and prioritize fixes based on severity. Lux is maintained by a small team, so timelines vary, but we treat security issues as our highest priority when they come in.

## What Qualifies

- Authentication or authorization bypasses
- Data loss or corruption vulnerabilities
- Denial of service attacks against the server process
- Memory safety issues
- Information disclosure (credentials, customer data)
- Injection attacks (command injection, Lua sandbox escapes)

## What Does Not Qualify

- Vulnerabilities in dependencies that don't affect Lux in practice
- Issues that require physical access to the host machine
- Social engineering attacks
- Denial of service via expected behavior (e.g., KEYS on large datasets)
- Non-security bugs (crashes, incorrect results) -- please open a regular GitHub issue for these

## Disclosure

We will coordinate disclosure with the reporter. Once a fix is available, we will:

1. Release a patched version
2. Publish a GitHub Security Advisory
3. Credit the reporter (unless they prefer to remain anonymous)

We ask that you give us reasonable time to address the issue before public disclosure.

## Scope

This policy covers:

- The Lux database engine ([github.com/lux-db/lux](https://github.com/lux-db/lux))
- Lux Cloud ([luxdb.dev](https://luxdb.dev))
- The luxctl CLI
- The @luxdb/sdk npm package

## Contact

Pompeii Labs, Inc.
hello@pompeiilabs.com
