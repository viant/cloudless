package gcp

import (
	"context"
	"github.com/viant/cloudless/compute/cluster"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
	"time"
)

func Match(criteria *cluster.Criteria) ([]cluster.Instance, error) {

	ctx := context.Background()
	client, err := google.DefaultClient(ctx, compute.ComputeScope)
	if err != nil {
		return nil, err
	}
	s, err := compute.New(client)

	if err != nil {
		return nil, err
	}

	parms := s.Instances.List(criteria.Project, criteria.Zone)
	gcpInstances, err := parms.Do()
	if err != nil {
		return nil, err
	}

	instances := make([]cluster.Instance, 0)
	for _, inst := range gcpInstances.Items {
		tm, err := time.Parse(time.RFC3339, inst.LastStartTimestamp)
		if match(inst.Tags.Items, criteria.Tags) && inst.Status == okStatus && err == nil {
			instances = append(instances, cluster.Instance{
				Name:      inst.Name,
				PrivateIP: inst.NetworkInterfaces[0].NetworkIP,
				StartTime: tm,
			})
		}

	}
	return instances, nil
}

func match(aSlice, bSlice []string) bool {
	for _, el1 := range aSlice {
		for _, el2 := range bSlice {
			if el1 == el2 {
				return true
			}
		}
	}
	return false
}
