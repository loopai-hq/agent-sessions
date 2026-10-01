package web

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Server-rendered SVG charts.
//
// The dashboard ships Content-Security-Policy: script-src 'none', and the charts
// are not the reason to give that up — a charting library would be the single
// largest attack surface on a page whose whole job is rendering other people's
// transcripts. Inline SVG needs no script: the browser draws it, CSS classes
// colour it, and the hover affordances below are CSS opacity over pre-rendered
// elements.
//
// This file computes geometry and nothing else. The SVG itself is emitted by the
// "chart" block in partials.html, through html/template's contextual escaping,
// because this package bans template.HTML by guard test: hover labels are email
// addresses and display names, and the ban is only worth having if the charts
// obey it too.
//
// Hover follows the shape shared-crosshair tooltips have everywhere (Highcharts,
// ZingChart, Grafana): one invisible hit column per time bucket; hovering it
// shows a vertical rule at that bucket and a panel listing every series' value
// there. The panel anchors right of the rule and flips left in the last stretch
// of the chart, because a tooltip that clips at the edge is unreadable exactly
// where the newest data is.
//
// Geometry is a fixed logical canvas scaled to its container (viewBox + CSS
// width 100%), so the chart fills the page at every range instead of scrolling.
// The bucket count is bounded by construction — at most 24 — so a segment is
// never narrower than ~37 logical pixels.

// ChartView is a chart, fully laid out, ready for the template to draw.
type ChartView struct {
	W, H   int
	Grid   []float64 // y coordinates of horizontal gridlines
	GridX0 int       // where gridlines start (right edge of the y-axis gutter)
	YTop   string    // label on the top gridline
	YTopY  float64
	YZeroY float64
	LabelX float64 // right-aligned x for y-axis labels

	XLabels []ChartXLabel
	Bars    []ChartBar
	Lines   []ChartLine
	Hits    []ChartHit
}

// Empty reports there is nothing to draw, so the template can omit the SVG
// rather than render an axis around nothing.
func (c ChartView) Empty() bool { return len(c.Bars) == 0 && len(c.Lines) == 0 }

// ChartXLabel is one time caption on the x axis.
type ChartXLabel struct {
	X    float64
	Text string
}

// ChartBar is one bar, positioned.
type ChartBar struct {
	X, Y, W, H float64
	Color      int
}

// ChartLine is one series drawn as a path with visible points.
type ChartLine struct {
	Color int
	// D is M/L commands and coordinates only, built from floats, so attribute
	// escaping has nothing to change in it.
	D      string
	Points []ChartPoint
}

// ChartPoint is one dot on a line.
type ChartPoint struct {
	X, Y  float64
	Color int
}

// ChartHit is one hover column: the invisible rect that catches the pointer,
// the crosshair rule at the bucket's centre, and the tooltip panel of values.
type ChartHit struct {
	X0, W, RuleX                   float64
	PanelX, PanelY, PanelW, PanelH float64
	Title                          string
	Rows                           []ChartHitRow
}

// TitleY positions the panel's date line.
func (h ChartHit) TitleY() float64 { return h.PanelY + 17 }

// ChartHitRow is one series' value at this bucket.
type ChartHitRow struct {
	Y     float64
	SwX   float64
	TextX float64
	Color int
	// Sw reports whether the row carries a colour swatch. Single-series charts
	// leave it off rather than decorate one value with a legend for itself.
	Sw    bool
	Label string
	Value string
}

// series is one named sequence, aligned to the chart's buckets.
type series struct {
	Label  string
	Points []float64
	Color  int
}

// chartSpec is everything a chart needs to lay itself out.
type chartSpec struct {
	Days []time.Time
	// Kind names the bucket width, for captions: "hour", "day", "2d", "week".
	Kind string
	Bars bool
	// Stacked stacks multi-series bars in one column per bucket instead of
	// drawing only the first series. The y scale becomes the bucket SUM, since
	// that is the height a stacked column actually reaches.
	Stacked bool
	Fmt     func(float64) string
	Ser     []series
}

// The logical canvas. Rendered at CSS width 100%, so these are proportions
// rather than device pixels.
const (
	chartW     = 960
	chartH     = 200
	chartPad   = 10
	chartYAxis = 56
	chartXPad  = 8
)

// layoutChart turns a spec into drawable geometry.
func layoutChart(sp chartSpec) ChartView {
	if len(sp.Days) == 0 || len(sp.Ser) == 0 {
		return ChartView{}
	}
	if sp.Fmt == nil {
		sp.Fmt = func(v float64) string { return fmt.Sprintf("%.0f", v) }
	}

	var peak float64
	if sp.Stacked {
		for i := range sp.Days {
			var sum float64
			for _, s := range sp.Ser {
				if i < len(s.Points) {
					sum += s.Points[i]
				}
			}
			if sum > peak {
				peak = sum
			}
		}
	} else {
		for _, s := range sp.Ser {
			for _, p := range s.Points {
				if p > peak {
					peak = p
				}
			}
		}
	}
	if peak == 0 {
		peak = 1 // a flat zero chart still needs a scale to draw its axis
	}
	peak = niceCeil(peak)

	n := len(sp.Days)
	plotW := float64(chartW - chartYAxis - 2*chartXPad)
	segW := plotW / float64(n)
	plotH := float64(chartH - 2*chartPad - 18) // 18 leaves room for x captions
	x := func(i int) float64 { return float64(chartYAxis+chartXPad) + segW*(float64(i)+0.5) }
	y := func(v float64) float64 { return chartPad + plotH - (v/peak)*plotH }

	cv := ChartView{
		W: chartW, H: chartH,
		GridX0: chartYAxis,
		YTop:   sp.Fmt(peak),
		YTopY:  chartPad + 9,
		YZeroY: chartPad + plotH,
		LabelX: chartYAxis - 6,
	}
	for q := range 5 {
		cv.Grid = append(cv.Grid, chartPad+plotH*float64(q)/4)
	}

	// X captions, thinned so they cannot collide. The step is derived from how
	// wide the widest caption renders against how wide a segment is, rather
	// than from a guessed divisor — a guessed divisor is how ninety daily
	// captions ended up written over each other. Stepping is anchored at the
	// newest bucket so the caption that survives thinning is the one for now.
	const charW = 5.4 // logical px per character at the 10px axis font
	widest := 0
	for _, d := range sp.Days {
		if l := len(xCaption(d, sp.Kind)); l > widest {
			widest = l
		}
	}
	step := int(math.Ceil((float64(widest)*charW + 16) / segW))
	if step < 1 {
		step = 1
	}
	for i, d := range sp.Days {
		if (n-1-i)%step != 0 {
			continue
		}
		cv.XLabels = append(cv.XLabels, ChartXLabel{X: x(i), Text: xCaption(d, sp.Kind)})
	}

	switch {
	case sp.Bars && sp.Stacked:
		// One column per bucket, segments bottom-up in series order. Heights
		// accumulate in value space and each boundary is projected through y()
		// once, so rounding cannot open gaps between segments.
		bw := segW * 0.6
		for i := range n {
			var acc float64
			drawn := false
			for _, s := range sp.Ser {
				if i >= len(s.Points) || s.Points[i] <= 0 {
					continue
				}
				y0 := y(acc)
				acc += s.Points[i]
				y1 := y(acc)
				cv.Bars = append(cv.Bars, ChartBar{X: x(i) - bw/2, Y: y1, W: bw, H: y0 - y1, Color: s.Color % 8})
				drawn = true
			}
			// A zero bucket still gets a sliver, because "nothing happened" and
			// "no data drawn here" must not look identical.
			if !drawn {
				cv.Bars = append(cv.Bars, ChartBar{X: x(i) - bw/2, Y: chartPad + plotH - 1, W: bw, H: 1, Color: sp.Ser[0].Color % 8})
			}
		}
	case sp.Bars:
		s := sp.Ser[0]
		bw := segW * 0.6
		for i, v := range s.Points {
			if i >= n {
				break
			}
			by := y(v)
			h := chartPad + plotH - by
			// Zero-value bars still get a sliver, because "nothing happened"
			// and "no data drawn here" must not look identical.
			if h < 1 {
				h = 1
				by = chartPad + plotH - 1
			}
			cv.Bars = append(cv.Bars, ChartBar{X: x(i) - bw/2, Y: by, W: bw, H: h, Color: s.Color % 8})
		}
	default:
		for _, s := range sp.Ser {
			line := ChartLine{Color: s.Color % 8}
			var d strings.Builder
			for i, v := range s.Points {
				if i >= n {
					break
				}
				cmd := "L"
				if i == 0 {
					cmd = "M"
				}
				fmt.Fprintf(&d, "%s%.1f %.1f ", cmd, x(i), y(v))
				line.Points = append(line.Points, ChartPoint{X: x(i), Y: y(v), Color: s.Color % 8})
			}
			line.D = strings.TrimSpace(d.String())
			cv.Lines = append(cv.Lines, line)
		}
	}

	// Hover columns, one per bucket, laid out last so the template draws them
	// above the marks.
	const rowH, panelPad = 14.0, 8.0
	panelW := 130.0
	for _, s := range sp.Ser {
		if w := float64(len(s.Label))*5.6 + float64(len(sp.Fmt(peak)))*6 + 46; w > panelW {
			panelW = w
		}
	}
	multi := len(sp.Ser) > 1
	for i, d := range sp.Days {
		h := ChartHit{X0: x(i) - segW/2, W: segW, RuleX: x(i), Title: hoverTitle(d, sp.Kind)}
		for _, s := range sp.Ser {
			if i >= len(s.Points) {
				continue
			}
			h.Rows = append(h.Rows, ChartHitRow{
				Color: s.Color % 8, Sw: multi,
				Label: s.Label, Value: sp.Fmt(s.Points[i]),
			})
		}
		h.PanelW = panelW
		h.PanelH = panelPad + rowH*float64(len(h.Rows)+1) + 6
		h.PanelX = h.RuleX + 12
		if h.PanelX+h.PanelW > chartW-4 {
			h.PanelX = h.RuleX - 12 - h.PanelW
		}
		h.PanelY = chartPad + 2
		for r := range h.Rows {
			h.Rows[r].Y = h.PanelY + 17 + rowH*float64(r+1)
			h.Rows[r].SwX = h.PanelX + 8
			h.Rows[r].TextX = h.PanelX + 8
			if h.Rows[r].Sw {
				h.Rows[r].TextX = h.PanelX + 22
			}
		}
		cv.Hits = append(cv.Hits, h)
	}
	return cv
}

// xCaption renders an axis caption for one bucket.
func xCaption(d time.Time, kind string) string {
	switch kind {
	case "hour":
		return d.Format("15:04")
	case "week":
		return "w/c " + d.Format("2 Jan")
	default:
		return d.Format("2 Jan")
	}
}

// hoverTitle names a bucket the way a person would say it out loud.
func hoverTitle(d time.Time, kind string) string {
	switch kind {
	case "hour":
		return d.Format("Mon 2 Jan, 15:04") + "–" + d.Add(time.Hour).Format("15:04")
	case "2d":
		return d.Format("Mon 2") + "–" + d.AddDate(0, 0, 1).Format("Mon 2 Jan")
	case "week":
		return "week of " + d.Format("Mon 2 Jan")
	default:
		return d.Format("Mon 2 Jan")
	}
}

// niceCeil rounds up to 1, 2 or 5 times a power of ten, which is what makes the
// top gridline label a number a person can halve and quarter in their head.
func niceCeil(v float64) float64 {
	if v <= 0 {
		return 1
	}
	mag := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 5, 10} {
		if v <= m*mag {
			return m * mag
		}
	}
	return 10 * mag
}
