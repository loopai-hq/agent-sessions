package web

import (
	"strings"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/goldmark/text"
)

func (b Block) LongMessage() bool {
	if b.Kind == KindAssistant {
		return false
	}
	var length, lines int
	for _, segment := range b.Text {
		length += utf8.RuneCountInString(segment.Text)
		lines += strings.Count(segment.Text, "\n")
	}
	return length > 600 || lines > 8
}

func continuousPath(path string) bool {
	id, ok := strings.CutPrefix(path, "/sessions/")
	if !ok {
		return false
	}
	id, ok = strings.CutSuffix(id, "/conversation")
	return ok && id != "" && !strings.Contains(id, "/")
}

func (b Block) MessagePreview() []Segment {
	var text strings.Builder
	for _, segment := range b.Text {
		text.WriteString(segment.Text)
	}
	preview := []rune(strings.Join(strings.Fields(text.String()), " "))
	if len(preview) > 320 {
		return Plain(string(preview[:320]) + "…")
	}
	return Plain(string(preview))
}

// ConversationRun groups presentation only; blocks retain their original IDs,
// ordering, timestamps and event links, including at pagination boundaries.
type ConversationRun struct {
	Blocks  []Block
	Work    bool
	Errors  int
	Matched bool
}

func (g Group) Runs() []ConversationRun {
	var runs []ConversationRun
	for _, b := range g.Blocks {
		work := b.Kind != KindPrompt && (b.Kind != KindAssistant || len(b.Text) == 0)
		if !work || len(runs) == 0 || !runs[len(runs)-1].Work {
			runs = append(runs, ConversationRun{Work: work})
		}
		r := &runs[len(runs)-1]
		r.Blocks = append(r.Blocks, b)
		if b.Failed {
			r.Errors++
		}
		r.Matched = r.Matched || b.Matched()
	}
	return runs
}

func (g Group) Matched() bool {
	for _, b := range g.Blocks {
		if b.Matched() {
			return true
		}
	}
	return false
}

func (b Block) Matched() bool {
	fields := [][]Segment{b.Text}
	if b.Tool != nil {
		fields = append(fields, b.Tool.Input, b.Tool.Output)
	}
	for _, field := range fields {
		for _, s := range field {
			if s.Match {
				return true
			}
		}
	}
	return false
}

// Goldmark owns Markdown syntax; the template owns markup and escaping.
// Raw HTML stays visible as text, and images never trigger external fetches.
var conversationMarkdown = goldmark.New(goldmark.WithExtensions(extension.Table, extension.Strikethrough))

type MarkdownNode struct {
	Kind     string
	Text     string
	URL      string
	Level    int
	Ordered  bool
	Start    int
	Children []MarkdownNode
	Segments []Segment
}

func markdown(segments []Segment) []MarkdownNode {
	var source strings.Builder
	for _, segment := range segments {
		if segment.Match {
			return []MarkdownNode{{Kind: "plain", Segments: segments}}
		}
		source.WriteString(segment.Text)
	}
	raw := []byte(source.String())
	root := conversationMarkdown.Parser().Parse(text.NewReader(raw))
	return markdownNodes(root, raw)
}

func markdownNodes(parent ast.Node, source []byte) []MarkdownNode {
	var nodes []MarkdownNode
	for child := parent.FirstChild(); child != nil; child = child.NextSibling() {
		node := MarkdownNode{Kind: child.Kind().String()}
		switch n := child.(type) {
		case *ast.Text:
			node.Text = string(n.Segment.Value(source))
			if n.SoftLineBreak() || n.HardLineBreak() {
				node.Text += "\n"
			}
		case *ast.String:
			node.Text = string(n.Value)
		case *ast.Heading:
			node.Level = n.Level
		case *ast.Emphasis:
			node.Level = n.Level
		case *ast.List:
			node.Ordered, node.Start = n.IsOrdered(), n.Start
		case *ast.Link:
			if !html.IsDangerousURL(n.Destination) {
				node.URL = string(n.Destination)
			}
		case *ast.AutoLink:
			node.Text = string(n.Label(source))
			if !html.IsDangerousURL(n.URL(source)) {
				node.URL = string(n.URL(source))
			}
		case *ast.HTMLBlock:
			// The block's lines followed by its closure line when it has one,
			// which is what the deprecated HTMLBlock.Text returned.
			raw := n.Lines().Value(source)
			if n.HasClosure() {
				raw = append(raw, n.ClosureLine.Value(source)...)
			}
			node.Text = string(raw)
		case *ast.RawHTML:
			node.Text = string(n.Segments.Value(source))
		case *ast.FencedCodeBlock:
			node.Text = string(n.Lines().Value(source))
		case *ast.CodeBlock:
			node.Text = string(n.Lines().Value(source))
		}
		node.Children = markdownNodes(child, source)
		nodes = append(nodes, node)
	}
	return nodes
}
