package main

import (
	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/pipeline"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// pipelineSink is the production sink behind the hookSink seam, named so a
// test that replaced the seam can still reach it.
func pipelineSink(sp *spool.Spool) (capture.Sink, error) { return pipeline.NewSink(sp) }
