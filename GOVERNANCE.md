# Governance

loop-sessions is a single-vendor open source project: it was built at Loop
AI, the maintainers are employed there, and it is in production use there.
This page says how decisions are made and how that could change.

## Roles

- **Maintainers** ([MAINTAINERS.md](MAINTAINERS.md)) review and merge pull
  requests, triage issues, cut releases, hold the repository settings, and
  answer security reports. A maintainer can merge another maintainer's pull
  request; nobody merges their own without a review, except for a change to
  documentation or CI that another maintainer has approved in the issue.
- **Contributors** are everyone whose change has been merged. They are
  credited by the commit history; the original authors of the pre-public
  history are named in [AUTHORS.md](AUTHORS.md).
- **Users** are the organisations that run a server and the engineers whose
  laptops are enrolled. Their reports and requests set the roadmap's order
  more than anything else does.

## How decisions are made

Most decisions are made in the pull request that implements them, by the
maintainer who reviews it. Three kinds of change need an issue first, so the
design can be discussed before the code:

1. anything that changes what an event carries (`event.CaptureSchema`) or
   what the server derives (`store.DerivedSchema`); [docs/UPGRADES.md](docs/UPGRADES.md)
   explains what each costs;
2. anything that changes the release layout, which is one contract across
   the `Makefile`, `install/install.sh`, `internal/upgrade` and
   `server/app/download.go`;
3. a new dependency for the client, which is standard-library only.

When maintainers disagree, they talk until they agree; if they cannot, the
maintainer who has owned the affected area longest decides, and the decision
and its reasons are written into the pull request or the issue so the next
person can see why. There is no steering committee and no vote, because
there are not enough people for either to mean anything; this page changes
when that changes.

## Becoming a maintainer

A contributor who has landed several non-trivial changes, reviewed other
people's pull requests thoughtfully, and shown that they hold the project's
rules (no real identifiers, synthetic fixtures, the client stays
dependency-free, every change explains why) can be nominated by any
maintainer. The existing maintainers agree, the person is added to
[MAINTAINERS.md](MAINTAINERS.md) and `.github/CODEOWNERS`, and gets write
access. Employment at Loop AI is not required. A maintainer who has been
inactive for six months is moved to emeritus in MAINTAINERS.md, with
thanks, and can return by asking.

## Changing this document

By pull request, reviewed by every current maintainer.
