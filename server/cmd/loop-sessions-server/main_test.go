package main

import (
	"strings"
	"testing"
)

// TestRunRefusesToStartOnAnEnvironmentItCannotUse defends the entrypoint's only
// real behaviour: an unusable environment ends the process before anything is
// opened, listened on or migrated.
//
// The failure mode this prevents is a server that starts, passes the platform's
// TCP startup probe, is promoted to serve 100% of traffic, and then discovers at
// the first sign-in that it was never told which Firebase project to verify
// tokens against. That is an outage found by a user rather than by a deploy.
func TestRunRefusesToStartOnAnEnvironmentItCannotUse(t *testing.T) {
	// Every required name is emptied rather than assumed absent, so the test
	// says the same thing on a machine where a developer has some of them set.
	for _, name := range []string{
		"DATABASE_HOST", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD",
		"SESSION_KEY", "FIREBASE_PROJECT_ID", "FIREBASE_API_KEY",
		"ALLOWED_DOMAINS", "PUBLIC_URL",
	} {
		t.Setenv(name, "")
	}

	err := run()
	if err == nil {
		t.Fatal("run succeeded with nothing configured")
	}
	// main turns any error from run into a non-zero exit, and the message is
	// what an operator reads in Cloud Logging, so it has to name what is
	// missing rather than say that something is.
	if !strings.Contains(err.Error(), "DATABASE_HOST") || !strings.Contains(err.Error(), "SESSION_KEY") {
		t.Errorf("the failure does not name the missing variables: %v", err)
	}
}

// TestVersionIsAPackageVariableTheBuildCanStamp is a tripwire on the linker
// contract in the Dockerfile at the repository root, which passes
// -ldflags "-X main.version=...". A -X against a symbol that does not exist is
// ignored silently, so a rename here would leave every deployed revision
// logging "dev" with nothing to say why.
func TestVersionIsAPackageVariableTheBuildCanStamp(t *testing.T) {
	if version == "" {
		t.Fatal("version must carry a default; an unstamped build has to log something")
	}
	version = "test-stamp"
	if version != "test-stamp" {
		t.Fatal("version is not writable, so the linker cannot stamp it")
	}
	version = "dev"
}
