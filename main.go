package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/theckman/yacspin"

	"github.com/cli/go-gh/v2/pkg/auth"
	"github.com/cli/go-gh/v2/pkg/repository"
	"github.com/google/go-github/v80/github"
)

type RunSummary struct {
	Run     *github.WorkflowRun
	Jobs    []*github.WorkflowJob
	JobsErr error
}

type WorkflowSummary struct {
	Workflow *github.Workflow
	Runs     []*github.WorkflowRun
	RunSums  map[int64]*RunSummary
}

func main() {
	// Get the client

	host, _ := auth.DefaultHost()
	token, _ := auth.TokenForHost(host)
	ghClient := github.NewClient(nil).WithAuthToken(token)

	cRepo := getTargetRepo()

	workflows := fetchWorkflows(ghClient, cRepo)
	summaries := fetchRunsForWorkflows(ghClient, cRepo, workflows)

	printSummaries(ghClient, cRepo, summaries)
}

// Determine the target repository either from command line argument or current directory
// Exits the program if no repository can be determined
func getTargetRepo() *repository.Repository {

	var targetRepo *repository.Repository

	// Check command line argument

	if len(os.Args) > 1 {
		parts := strings.Split(os.Args[1], "/")
		if len(parts) == 2 {
			targetRepo = &repository.Repository{Owner: parts[0], Name: parts[1]}
		} else {
			fmt.Println("invalid repository format, expected owner/name")
			os.Exit(1)
		}
	} else {

		cRepo, err := repository.Current()

		if err != nil {
			fmt.Println("no current repository found, please specify a repository as owner/name")
			os.Exit(1)
		}

		targetRepo = &cRepo
	}
	return targetRepo
}

func fetchWorkflows(ghClient *github.Client, cRepo *repository.Repository) *github.Workflows {
	workflows, _, err := ghClient.Actions.ListWorkflows(context.Background(), cRepo.Owner, cRepo.Name, &github.ListOptions{})
	if err != nil {
		fmt.Printf("error fetching workflows: %v\n", err)
		os.Exit(1)
	}
	return workflows
}

func fetchRunsForWorkflows(ghClient *github.Client, cRepo *repository.Repository, workflows *github.Workflows) []*WorkflowSummary {
	var wg sync.WaitGroup
	summaries := make([]*WorkflowSummary, 0, len(workflows.Workflows))
	mu := sync.Mutex{}

	var runsTotal int64 = int64(len(workflows.Workflows))
	var runsDone int64
	var jobsTotal int64
	var jobsDone int64

	done := make(chan struct{})
	cfg := yacspin.Config{
		Frequency: 150 * time.Millisecond,
		Writer:    os.Stdout,
		CharSet:   yacspin.CharSets[14],
	}
	s, serr := yacspin.New(cfg)
	if serr == nil {
		s.Start()
		go func() {
			t := time.NewTicker(150 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-done:
					s.Stop()
					return
				case <-t.C:
					rDone := atomic.LoadInt64(&runsDone)
					rTot := atomic.LoadInt64(&runsTotal)
					jDone := atomic.LoadInt64(&jobsDone)
					jTot := atomic.LoadInt64(&jobsTotal)
					s.Message(fmt.Sprintf(" Fetching runs %d/%d jobs %d/%d", rDone, rTot, jDone, jTot))
				}
			}
		}()
	} else {
		panic(serr)
	}

	for _, wf := range workflows.Workflows {
		wf := wf
		wg.Add(1)
		go func() {
			defer wg.Done()
			runsResp, _, err := ghClient.Actions.ListWorkflowRunsByID(context.Background(), cRepo.Owner, cRepo.Name, wf.GetID(), &github.ListWorkflowRunsOptions{ListOptions: github.ListOptions{PerPage: 50}})
			if err != nil {
				fmt.Printf("\n  error fetching runs for workflow %s: %v\n", wf.GetName(), err)
				atomic.AddInt64(&runsDone, 1)
				return
			}
			if len(runsResp.WorkflowRuns) == 0 {
				atomic.AddInt64(&runsDone, 1)
				return
			}

			ws := &WorkflowSummary{Workflow: wf, Runs: runsResp.WorkflowRuns, RunSums: map[int64]*RunSummary{}}

			var innerWg sync.WaitGroup
			for _, run := range ws.Runs {
				if run == nil {
					continue
				}
				runID := run.GetID()
				rs := &RunSummary{Run: run}
				ws.RunSums[runID] = rs
				if run.Status != nil && *run.Status == "in_progress" {
					atomic.AddInt64(&jobsTotal, 1)
					innerWg.Add(1)
					go func(r *github.WorkflowRun, rs *RunSummary) {
						defer innerWg.Done()
						defer atomic.AddInt64(&jobsDone, 1)
						jobsResp, _, jerr := ghClient.Actions.ListWorkflowJobs(context.Background(), cRepo.Owner, cRepo.Name, r.GetID(), nil)
						if jerr != nil {
							rs.JobsErr = jerr
							return
						}
						rs.Jobs = jobsResp.Jobs
					}(run, rs)
				}
			}
			innerWg.Wait()

			mu.Lock()
			summaries = append(summaries, ws)
			mu.Unlock()

			atomic.AddInt64(&runsDone, 1)
		}()
	}
	wg.Wait()
	close(done)

	sort.SliceStable(summaries, func(i, j int) bool {
		return strings.ToLower(summaries[i].Workflow.GetName()) < strings.ToLower(summaries[j].Workflow.GetName())
	})
	return summaries
}

func printSummaries(ghClient *github.Client, cRepo *repository.Repository, summaries []*WorkflowSummary) {
	for _, ws := range summaries {
		wf := ws.Workflow
		fmt.Printf("- %s (ID: %d)\n", wf.GetName(), wf.GetID())

		glyphs := make([]string, 0, len(ws.Runs))
		for _, run := range ws.Runs {
			if run == nil {
				continue
			}
			status := run.GetStatus()
			conclusion := run.GetConclusion()
			switch {
			case status == "in_progress":
				glyphs = append(glyphs, "▶️")
			case status == "completed" && conclusion == "success":
				glyphs = append(glyphs, "✅")
			case status == "completed" && conclusion == "failure":
				glyphs = append(glyphs, "🆖")
			case status == "completed" && conclusion == "cancelled":
				glyphs = append(glyphs, "⏹️")
			default:
				glyphs = append(glyphs, "*️⃣")
			}
		}
		fmt.Println("  Chart:", strings.Join(glyphs, ""))

		most := ws.Runs[0]
		fmt.Printf("  Most recent: #%d status=%s conclusion=%s url=%s\n", most.GetRunNumber(), most.GetStatus(), most.GetConclusion(), most.GetHTMLURL())

		if most.GetStatus() == "completed" && most.GetConclusion() == "failure" {
			fmt.Println("  Failure analysis:")
			jobsResp, _, jerr := ghClient.Actions.ListWorkflowJobs(context.Background(), cRepo.Owner, cRepo.Name, most.GetID(), nil)
			if jerr != nil {
				fmt.Printf("    error fetching jobs for run %d: %v\n", most.GetID(), jerr)
			} else {
				for _, job := range jobsResp.Jobs {
					if job.GetConclusion() != "success" {
						fmt.Printf("    Job: %s - conclusion=%s status=%s\n", job.GetName(), job.GetConclusion(), job.GetStatus())
						for _, step := range job.Steps {
							if step.GetConclusion() != "success" {
								fmt.Printf("      Step: %s - conclusion=%s status=%s\n", step.GetName(), step.GetConclusion(), step.GetStatus())
								break
							}
						}
					}
				}
			}
		}

		inProgressFound := false
		for _, run := range ws.Runs {
			if run == nil {
				continue
			}
			if run.GetStatus() == "in_progress" {
				inProgressFound = true
				rs := ws.RunSums[run.GetID()]
				fmt.Printf("  In-progress run #%d (id=%d):\n", run.GetRunNumber(), run.GetID())
				if rs == nil {
					fmt.Println("    (no details)")
					continue
				}
				if rs.JobsErr != nil {
					fmt.Printf("    error fetching jobs: %v\n", rs.JobsErr)
					continue
				}
				for _, job := range rs.Jobs {
					fmt.Printf("    Job: %s status=%s conclusion=%s\n", job.GetName(), job.GetStatus(), job.GetConclusion())
					for _, step := range job.Steps {
						fmt.Printf("      Step: %s status=%s conclusion=%s\n", step.GetName(), step.GetStatus(), step.GetConclusion())
					}
				}
			}
		}
		if !inProgressFound {
			// nothing
		}
	}
}
