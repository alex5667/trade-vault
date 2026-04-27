package tradeengine

import (
	"context"
	"go-worker/pkg/tradeevents"
)

type Engine struct {
	publisher *tradeevents.Publisher
}

func NewEngine(publisher *tradeevents.Publisher) *Engine {
	return &Engine{publisher: publisher}
}

func (e *Engine) OnPositionClosed(ctx context.Context, tr *TradeState, rMult float64) error {
	meta := &tradeevents.MetaEnforceInfo{CovBucket: "unknown", Applied: false}
	if tr != nil && tr.MetaEnforce != nil {
		meta.CovBucket = tr.MetaEnforce.CovBucket
		meta.Applied = tr.MetaEnforce.Applied
	}
	return e.publisher.PublishPositionClosed(ctx, tr.Symbol, tr.SID, rMult, meta)
}
