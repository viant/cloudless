package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/viant/cloudless/compute/cluster"
	"strings"
)

func Match(criteria *cluster.Criteria) ([]cluster.Instance, error) {
	sess := session.New()
	sess.Config.Region = &criteria.Region
	svc := ec2.New(sess)

	var kvPairs = make(map[string]string)
	var values = make([]string, 0)
	for _, tag := range criteria.Tags {
		pair := strings.SplitN(tag, ":", 2)
		switch len(pair) {
		case 1:
			values = append(values, pair[0])
		case 2:
			values = append(values, pair[1])
			kvPairs[pair[0]] = pair[1]
		}
	}
	input := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("tag-value"),
				Values: aws.StringSlice(values),
			},
		},
	}

	result, err := svc.DescribeInstances(input)
	if err != nil {
		return nil, err
	}

	instances := make([]cluster.Instance, 0)
	for i := range result.Reservations {
		for _, inst := range result.Reservations[i].Instances {
			if !matchKVPairs(inst.Tags, kvPairs) {
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

func matchKVPairs(tags []*ec2.Tag, pairs map[string]string) bool {
	if len(pairs) == 0 {
		return true
	}
	for _, tag := range tags {
		candidateValue, ok := pairs[*tag.Key]
		if !ok {
			continue
		}
		if candidateValue != *tag.Value {
			return false
		}
	}
	return true
}
