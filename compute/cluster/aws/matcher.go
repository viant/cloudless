package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/viant/cloudless/compute/cluster"
	"strings"
)

func Match(criteria *cluster.Criteria) ([]cluster.Instance, error) {
	sess := session.New()
	sess.Config.Region = &criteria.Region
	svc := ec2.New(sess)

	exclusions, filters := buildFilters(criteria)
	result, err := svc.DescribeInstances(&ec2.DescribeInstancesInput{Filters: filters})
	if err != nil {
		return nil, err
	}

	instances := make([]cluster.Instance, 0)
	for i := range result.Reservations {
		for _, inst := range result.Reservations[i].Instances {
			if exclude(inst.Tags, exclusions) {
				continue
			}

			if *inst.State.Name == okStatus {
				instances = append(instances, cluster.Instance{
					Name:      *inst.InstanceId,
					PrivateIP: *inst.PrivateIpAddress,
					StartTime: *inst.LaunchTime,
				})
			}
		}
	}
	return instances, nil
}

func buildFilters(criteria *cluster.Criteria) (map[string]bool, []*ec2.Filter) {
	var exclusions = make(map[string]bool)
	var tags = make(map[string][]*string)
	for _, tag := range criteria.Tags {
		if tag[0:1] == "!" {
			exclusions[tag[1:]] = true
			continue
		}
		pair := strings.SplitN(tag, ":", 2)
		switch len(pair) {
		case 1:
			tags["tag-value"] = append(tags["tag-value"], &pair[0])
		case 2:
			tags["tag:"+pair[0]] = append(tags["tag:"+pair[0]], &pair[1])
		}
	}

	var filters = make([]*ec2.Filter, 0)
	for n, v := range tags {
		name := n
		filters = append(filters, &ec2.Filter{
			Name:   &name,
			Values: v,
		})
	}
	return exclusions, filters
}

func exclude(tags []*ec2.Tag, exclusions map[string]bool) bool {
	if len(exclusions) == 0 {
		return false
	}
	for _, tag := range tags {
		_, ok := exclusions[*tag.Value]
		if ok {
			return true
		}
		_, ok = exclusions[*tag.Key+":"+*tag.Value]
		if ok {
			return true
		}
	}
	return false
}
