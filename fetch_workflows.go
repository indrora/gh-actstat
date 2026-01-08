package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cli/go-gh/v2/pkg/repository"
	"github.com/google/go-github/v80/github"
)

func fetchWorkflows(ghClient *github.Client, cRepo *repository.Repository) *github.Workflows {

	var workflows *github.Workflows
	var err error

	err = runWithSpinner("Fetching workflows", func(c chan<- string) error {
		workflows, _, err = ghClient.Actions.ListWorkflows(context.Background(), cRepo.Owner, cRepo.Name, &github.ListOptions{})
		return err
	})

	if err != nil {
		fmt.Printf("error fetching workflows: %v\n", err)
		os.Exit(1)
	}

	return workflows
}
