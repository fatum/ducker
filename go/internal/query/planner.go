package query

import (
	"fmt"
	"strings"

	"ducker/internal/bloom"
	"ducker/internal/model"
)

type BloomSource interface {
	GetBloomData(tenantID, segment string) (bloom.FileBloom, bool, error)
}

type ColdBloomSource interface {
	GetBloom(tenantID, segment string) (map[string]any, error)
}

type Planner struct {
	coldStorage ColdBloomSource
	dbCache     BloomSource
}

func NewPlanner(cold ColdBloomSource, cache BloomSource) *Planner {
	return &Planner{coldStorage: cold, dbCache: cache}
}

func (p *Planner) Plan(tenantID string, manifest model.Manifest, startTS, endTS int64, filters map[string]any) (model.QueryPlan, error) {
	totalFiles := len(manifest.Files)
	timeFiltered := make([]model.SegmentFile, 0, totalFiles)
	for _, f := range manifest.Files {
		if f.EndTS >= startTS && f.StartTS <= endTS {
			timeFiltered = append(timeFiltered, f)
		}
	}

	bloomFilters := extractBloomFilters(filters)
	hasBloomFilters := len(bloomFilters) > 0
	bloomFiltered := timeFiltered

	if hasBloomFilters {
		bloomFiltered = make([]model.SegmentFile, 0, len(timeFiltered))
		for _, file := range timeFiltered {
			data, ok, err := p.getBloomData(tenantID, file.Segment)
			if err != nil || !ok || bloom.ProbeFileBloom(data, bloomFilters) {
				bloomFiltered = append(bloomFiltered, file)
			}
		}
	}

	return model.QueryPlan{
		Files: bloomFiltered,
		Stats: model.PlanStats{
			TotalFiles:           totalFiles,
			FilesAfterTimeFilter: len(timeFiltered),
			FilesAfterBloom:      len(bloomFiltered),
		},
	}, nil
}

func extractBloomFilters(filters map[string]any) map[string]any {
	bloomFilters := map[string]any{}
	for k, v := range filters {
		s, ok := v.(string)
		if ok {
			if !strings.Contains(s, "*") && !strings.Contains(s, "?") {
				bloomFilters[k] = s
			}
			continue
		}
		if arr, ok := v.([]any); ok {
			allStrings := true
			vals := make([]string, 0, len(arr))
			for _, item := range arr {
				str, ok := item.(string)
				if !ok {
					allStrings = false
					break
				}
				vals = append(vals, str)
			}
			if allStrings {
				bloomFilters[k] = vals
			}
		}
	}
	return bloomFilters
}

func (p *Planner) getBloomData(tenantID, segment string) (bloom.FileBloom, bool, error) {
	if p.dbCache != nil {
		if data, ok, err := p.dbCache.GetBloomData(tenantID, segment); err == nil && ok {
			return data, true, nil
		}
	}
	raw, err := p.coldStorage.GetBloom(tenantID, segment)
	if err != nil {
		return bloom.FileBloom{}, false, nil
	}
	parsed, err := bloom.ParseFileBloom(raw)
	if err != nil {
		return bloom.FileBloom{}, false, fmt.Errorf("parse bloom: %w", err)
	}
	return parsed, true, nil
}
