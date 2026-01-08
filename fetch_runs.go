package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cli/go-gh/v2/pkg/repository"
	"github.com/google/go-github/v80/github"
)

func fetchRunsForWorkflows(ghClient *github.Client, cRepo *repository.Repository, workflows *github.Workflows) []*WorkflowSummary {
	var runsTotal int = len(workflows.Workflows)
	var runsDone int
	var summaries []*WorkflowSummary

	spinFunc := func(progress chan<- string) error {
		for _, wf := range workflows.Workflows {
			runsResp, _, err := ghClient.Actions.ListWorkflowRunsByID(context.Background(), cRepo.Owner, cRepo.Name, wf.GetID(), &github.ListWorkflowRunsOptions{ListOptions: github.ListOptions{PerPage: 50}})
			progress <- fmt.Sprintf(" Fetching runs %d/%d", runsDone+1, runsTotal)
			if err != nil {
				fmt.Printf("\n  error fetching runs for workflow %s: %v\n", wf.GetName(), err)
				runsDone++
				continue
			}
			if len(runsResp.WorkflowRuns) == 0 {
				runsDone++
				continue
			}

			ws := &WorkflowSummary{Workflow: wf, Runs: runsResp.WorkflowRuns, RunSums: map[int64]*RunSummary{}}
			for _, run := range ws.Runs {
				if run == nil {
					continue
				}
				runID := run.GetID()
				rs := &RunSummary{Run: run}
				ws.RunSums[runID] = rs
			}

			summaries = append(summaries, ws)

			runsDone++

		}
		return nil
	}

	runWithSpinner("Fetching runs", spinFunc)

	sort.SliceStable(summaries, func(i, j int) bool {
		return strings.ToLower(summaries[i].Workflow.GetName()) < strings.ToLower(summaries[j].Workflow.GetName())
	})
	return summaries
}
