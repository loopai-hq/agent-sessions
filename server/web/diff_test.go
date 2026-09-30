package web

import (
	"strings"
	"testing"
)

func kinds(fd FileDiff) string {
	var b strings.Builder
	for _, l := range fd.Lines {
		switch l.Kind {
		case DiffAdd:
			b.WriteByte('+')
		case DiffRemove:
			b.WriteByte('-')
		case DiffContext:
			b.WriteByte('=')
		case DiffElision:
			b.WriteByte('~')
		}
	}
	return b.String()
}

func TestDiffAlignsASmallEdit(t *testing.T) {
	before := "one\ntwo\nthree\n"
	after := "one\ntwo point five\nthree\n"
	fd := BuildFileDiff("f.go", before, after, false)

	if got := kinds(fd); got != "=-+=" {
		t.Fatalf("shape = %q, want a single replaced line between context", got)
	}
	if fd.Added != 1 || fd.Removed != 1 {
		t.Errorf("counts = +%d -%d", fd.Added, fd.Removed)
	}
	if fd.Lines[1].OldLine != 2 || fd.Lines[1].NewLine != 0 {
		t.Errorf("removed line numbering = %+v", fd.Lines[1])
	}
	if fd.Lines[2].NewLine != 2 || fd.Lines[2].OldLine != 0 {
		t.Errorf("added line numbering = %+v", fd.Lines[2])
	}
	if fd.Coarse || fd.Truncated {
		t.Error("a three line file should need neither fallback")
	}
}

func TestDiffOfANewFileIsAllAdditions(t *testing.T) {
	fd := BuildFileDiff("new.go", "", "package main\nfunc main() {}\n", true)
	if got := kinds(fd); got != "++" {
		t.Fatalf("shape = %q", got)
	}
	if !fd.Created || fd.Removed != 0 {
		t.Errorf("created = %v, removed = %d", fd.Created, fd.Removed)
	}
}

func TestDiffCollapsesLongUnchangedRuns(t *testing.T) {
	var before, after []string
	for range 200 {
		before = append(before, "line")
		after = append(after, "line")
	}
	after[100] = "changed"
	fd := BuildFileDiff("big.go", strings.Join(before, "\n"), strings.Join(after, "\n"), false)

	if !strings.Contains(kinds(fd), "~") {
		t.Fatal("unchanged runs were not collapsed")
	}
	if len(fd.Lines) > 20 {
		t.Errorf("rendered %d lines for a one line change", len(fd.Lines))
	}
	var elided string
	for _, l := range fd.Lines {
		if l.Kind == DiffElision {
			elided = l.Text
			break
		}
	}
	if !strings.Contains(elided, "unchanged lines") {
		t.Errorf("elision marker = %q, it must say how much was skipped", elided)
	}
}

func TestDiffFallsBackToBlockReplaceWhenTooLargeToAlign(t *testing.T) {
	var before, after []string
	for range diffAlignLimit + 50 {
		before = append(before, "a")
		after = append(after, "b")
	}
	fd := BuildFileDiff("huge.go", strings.Join(before, "\n"), strings.Join(after, "\n"), false)

	if !fd.Coarse {
		t.Fatal("an unalignable diff was not reported as coarse")
	}
	if fd.Added == 0 || fd.Removed == 0 {
		t.Errorf("coarse diff lost its counts: +%d -%d", fd.Added, fd.Removed)
	}
}

func TestDiffCapsItsOutput(t *testing.T) {
	var before, after []string
	for range diffMaxLines + 2000 {
		before = append(before, "x")
		after = append(after, "y")
	}
	fd := BuildFileDiff("massive.go", strings.Join(before, "\n"), strings.Join(after, "\n"), false)

	if !fd.Truncated {
		t.Fatal("an oversized file was not reported as shortened")
	}
	if len(fd.Lines) > diffMaxRenderLines {
		t.Fatalf("rendered %d lines, above the ceiling", len(fd.Lines))
	}
}

func TestDiffHandlesPureAppendAndPureDelete(t *testing.T) {
	appended := BuildFileDiff("f", "a\nb\n", "a\nb\nc\n", false)
	if appended.Added != 1 || appended.Removed != 0 {
		t.Errorf("append: +%d -%d", appended.Added, appended.Removed)
	}
	deleted := BuildFileDiff("f", "a\nb\nc\n", "a\nc\n", false)
	if deleted.Added != 0 || deleted.Removed != 1 {
		t.Errorf("delete: +%d -%d", deleted.Added, deleted.Removed)
	}
}

func TestDiffIgnoresTheTrailingNewline(t *testing.T) {
	fd := BuildFileDiff("f", "one\ntwo", "one\ntwo\n", false)
	if fd.Added != 0 || fd.Removed != 0 {
		t.Fatalf("a trailing newline counted as a change: +%d -%d", fd.Added, fd.Removed)
	}
}

func TestDiffSaysSoWhenNothingChanged(t *testing.T) {
	fd := BuildFileDiff("f", "a\nb\n", "a\nb\n", false)
	if len(fd.Lines) != 1 || fd.Lines[0].Kind != DiffElision || fd.Lines[0].Text != "no changes" {
		t.Fatalf("an unchanged edit rendered as %q", kinds(fd))
	}
}
