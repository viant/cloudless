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
		if (matchLabels(criteria.Labels, inst.Labels) || matchTags(criteria.Tags, inst.Tags.Items)) && inst.Status == okStatus && err == nil {
			instances = append(instances, cluster.Instance{
				Name:      inst.Name,
				PrivateIP: inst.NetworkInterfaces[0].NetworkIP,
				StartTime: tm,
			})
		}
	}
	return instances, nil
}

// OR logic for multiple tags
func matchTags(aSlice, bSlice []string) bool {
	for _, el1 := range aSlice {
		for _, el2 := range bSlice {
			if el1 == el2 {
				return true
			}
		}
	}
	return false
}

// AND logic for multiple labels
func matchLabels(aMap, bMap map[string]string) bool {
	for k, v := range aMap {
		if v != bMap[k] {
			return false
		}
	}
	return true
}
