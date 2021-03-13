package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/viant/cloudless/compute/cluster"
)

func init() {
	cluster.Register("AWS", Match)
}

func Match(criteria *cluster.Criteria) ([]cluster.Instance, error) {
	svc := ec2.New(session.New())
	svc.Config.Region = &criteria.Region

	input := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("tag-value"),
				Values: aws.StringSlice(criteria.Tags),
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
			if *inst.State.Name == "running" {
				instances = append(instances, cluster.Instance{
					Name:      *inst.InstanceId,
					PrivateIP: *inst.PrivateIpAddress,
				})
			}
		}
	}
	return instances, nil
}
