package web

import (
	"fmt"
	"strings"
)

// DiffKind classifies a rendered diff line.
type DiffKind string

const (
	// DiffContext is an unchanged line kept for orientation.
	DiffContext DiffKind = "ctx"
	// DiffAdd is a line present only after the edit.
	DiffAdd DiffKind = "add"
	// DiffRemove is a line present only before it.
	DiffRemove DiffKind = "del"
	// DiffElision stands in for a run of unchanged lines that was collapsed,
	// carrying the count so the reader knows how much was skipped instead of
	// wondering whether the file simply jumps.
	DiffElision DiffKind = "gap"
)

// DiffLine is one rendered row of a file diff. Line numbers are the ones from
// each side of the edit and are zero where the line does not exist on that
// side, which is what lets the template leave the gutter blank rather than
// printing a misleading number.
type DiffLine struct {
	Kind    DiffKind
	Text    string
	OldLine int
	NewLine int
}

// FileDiff is a rendered change to one file.
type FileDiff struct {
	Path    string
	Created bool
	Added   int
	Removed int
	Lines   []DiffLine

	// Truncated reports that the file was too large to diff in full. The
	// alternative to saying so is a diff that quietly lies about what changed,
	// which in an audit surface is worse than an incomplete one.
	Truncated bool
	// Coarse reports that the change was too large to align line by line, so
	// the whole region is shown as a removal followed by an addition. Named
	// rather than hidden because a coarse diff looks like a total rewrite and
	// a reader deserves to know which it is.
	Coarse bool
}

const (
	// diffMaxLines is the per-side line ceiling. Above it we stop reading the
	// file: nobody reviews a 20,000 line diff in a transcript viewer, and the
	// rendering cost is paid on every page load of that session.
	diffMaxLines = 4000
	// diffAlignLimit bounds the quadratic part. The LCS table is
	// int32[a+1][b+1], so 600x600 is about 1.4 MB and a few milliseconds,
	// while 4000x4000 would be 64 MB and visibly slow.
	diffAlignLimit = 600
	// diffContextLines is how many unchanged lines surround each change.
	diffContextLines = 3
	// diffMaxRenderLines caps the output regardless of input, so one
	// pathological file cannot make a page unbounded.
	diffMaxRenderLines = 1200
)

// BuildFileDiff renders a before/after pair into displayable lines.
//
// The pipeline is trim, align, then collapse. Trimming the common prefix and
// suffix first is what makes the expensive step affordable: real edits change a
// handful of lines in the middle of a file, so after trimming, the region that
// needs true alignment is almost always tiny even when the file is not. When it
// is not tiny, the function degrades to a coarse block replacement and says so,
// which is the honest failure mode.
func BuildFileDiff(path, before, after string, created bool) FileDiff {
	fd := FileDiff{Path: path, Created: created}

	a, aTrunc := splitLines(before, diffMaxLines)
	b, bTrunc := splitLines(after, diffMaxLines)
	fd.Truncated = aTrunc || bTrunc

	// A creation has no meaningful "before" even when the field is populated
	// with an empty string, and rendering every line as an addition is both
	// correct and cheaper than aligning against nothing.
	if created && len(a) == 0 {
		for i, line := range b {
			fd.Lines = append(fd.Lines, DiffLine{Kind: DiffAdd, Text: line, NewLine: i + 1})
			fd.Added++
			if len(fd.Lines) >= diffMaxRenderLines {
				fd.Truncated = true
				break
			}
		}
		return fd
	}

	prefix := 0
	for prefix < len(a) && prefix < len(b) && a[prefix] == b[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(a)-prefix && suffix < len(b)-prefix &&
		a[len(a)-1-suffix] == b[len(b)-1-suffix] {
		suffix++
	}

	midA := a[prefix : len(a)-suffix]
	midB := b[prefix : len(b)-suffix]

	var mid []DiffLine
	if len(midA) > diffAlignLimit || len(midB) > diffAlignLimit {
		fd.Coarse = true
		for i, line := range midA {
			mid = append(mid, DiffLine{Kind: DiffRemove, Text: line, OldLine: prefix + i + 1})
		}
		for i, line := range midB {
			mid = append(mid, DiffLine{Kind: DiffAdd, Text: line, NewLine: prefix + i + 1})
		}
	} else {
		mid = alignLines(midA, midB, prefix)
	}

	head := make([]DiffLine, 0, prefix)
	for i := range prefix {
		head = append(head, DiffLine{Kind: DiffContext, Text: a[i], OldLine: i + 1, NewLine: i + 1})
	}
	tail := make([]DiffLine, 0, suffix)
	for i := range suffix {
		oldIdx := len(a) - suffix + i
		newIdx := len(b) - suffix + i
		tail = append(tail, DiffLine{Kind: DiffContext, Text: a[oldIdx], OldLine: oldIdx + 1, NewLine: newIdx + 1})
	}

	all := append(append(head, mid...), tail...)
	for _, l := range all {
		switch l.Kind {
		case DiffAdd:
			fd.Added++
		case DiffRemove:
			fd.Removed++
		}
	}

	if fd.Added == 0 && fd.Removed == 0 {
		// A tool that reported an edit which changed nothing is worth showing
		// as exactly that. Collapsing it to context lines would render a box
		// full of unchanged file and leave the reader hunting for the change.
		fd.Lines = []DiffLine{{Kind: DiffElision, Text: "no changes"}}
		return fd
	}

	fd.Lines = collapseContext(all, diffContextLines)
	if len(fd.Lines) > diffMaxRenderLines {
		fd.Lines = fd.Lines[:diffMaxRenderLines]
		fd.Truncated = true
	}
	return fd
}

// alignLines is a longest-common-subsequence diff over the already-trimmed
// middle region. offset is how many lines were trimmed from the front, so the
// emitted line numbers refer to the real file rather than to the slice.
func alignLines(a, b []string, offset int) []DiffLine {
	n, m := len(a), len(b)
	if n == 0 && m == 0 {
		return nil
	}
	// table[i][j] is the LCS length of a[i:] and b[j:]. Walking backwards
	// means the reconstruction below runs forwards, which is the order the
	// lines are displayed in.
	table := make([][]int32, n+1)
	buf := make([]int32, (n+1)*(m+1))
	for i := range table {
		table[i] = buf[i*(m+1) : (i+1)*(m+1)]
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				table[i][j] = table[i+1][j+1] + 1
			} else if table[i+1][j] >= table[i][j+1] {
				table[i][j] = table[i+1][j]
			} else {
				table[i][j] = table[i][j+1]
			}
		}
	}

	var out []DiffLine
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			out = append(out, DiffLine{Kind: DiffContext, Text: a[i], OldLine: offset + i + 1, NewLine: offset + j + 1})
			i++
			j++
		case table[i+1][j] >= table[i][j+1]:
			out = append(out, DiffLine{Kind: DiffRemove, Text: a[i], OldLine: offset + i + 1})
			i++
		default:
			out = append(out, DiffLine{Kind: DiffAdd, Text: b[j], NewLine: offset + j + 1})
			j++
		}
	}
	for ; i < n; i++ {
		out = append(out, DiffLine{Kind: DiffRemove, Text: a[i], OldLine: offset + i + 1})
	}
	for ; j < m; j++ {
		out = append(out, DiffLine{Kind: DiffAdd, Text: b[j], NewLine: offset + j + 1})
	}
	return out
}

// collapseContext replaces long runs of unchanged lines with an elision
// marker. Without it, a one-line change to a large file renders as the entire
// file, and the change itself is somewhere in the middle of it.
func collapseContext(in []DiffLine, context int) []DiffLine {
	keep := make([]bool, len(in))
	for i, l := range in {
		if l.Kind == DiffContext {
			continue
		}
		lo := i - context
		if lo < 0 {
			lo = 0
		}
		hi := i + context
		if hi >= len(in) {
			hi = len(in) - 1
		}
		for k := lo; k <= hi; k++ {
			keep[k] = true
		}
	}

	var out []DiffLine
	skipped := 0
	for i, l := range in {
		if keep[i] {
			if skipped > 0 {
				out = append(out, DiffLine{
					Kind: DiffElision,
					Text: fmt.Sprintf("%d unchanged lines", skipped),
				})
				skipped = 0
			}
			out = append(out, l)
			continue
		}
		skipped++
	}
	if skipped > 0 {
		out = append(out, DiffLine{Kind: DiffElision, Text: fmt.Sprintf("%d unchanged lines", skipped)})
	}
	return out
}

// splitLines splits on newlines up to a ceiling, reporting whether it stopped
// early. A trailing newline does not produce a final empty line, because every
// well-formed file has one and rendering it as a line the user must scroll past
// is noise on every diff.
func splitLines(s string, max int) ([]string, bool) {
	if s == "" {
		return nil, false
	}
	s = strings.TrimSuffix(s, "\n")
	lines := strings.Split(s, "\n")
	if len(lines) > max {
		return lines[:max], true
	}
	return lines, false
}
