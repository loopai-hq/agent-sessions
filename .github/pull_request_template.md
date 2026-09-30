**What this changes and why**

**How you verified it**

Paste the relevant test output if CI does not obviously cover it.

**AI assistance:** none / `<tool>`: `<what it produced (code, tests, docs)>`

**Checklist**

- [ ] `make lint && go test -race ./...` passes locally
- [ ] `golangci-lint run ./...` clean (v2.14.0, the version CI runs)
- [ ] New behaviour has a test; scrub fixtures are synthetic
- [ ] Conventional commit subject; the body says why
- [ ] No company-specific identifiers, personal data or transcript content (the identifier gate will check the mechanical part)
- [ ] If the release layout or a schema constant changed, all the places that share the contract changed together
- [ ] I can explain every change without the tool
