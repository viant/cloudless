package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/viant/cloudless/compute/cluster"
)

func Lookup(criteria *cluster.Criteria) ([]cluster.Instance, error) {
	svc := ec2.New(session.New())
	svc.Config.Region = &criteria.Region
	//convert tags to a slice of pointers

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
			instance := cluster.Instance{}
			if *inst.State.Name == "running" {
				instance.Name = *inst.InstanceId
				instance.PrivateIP = *inst.PrivateIpAddress
			}
			instances = append(instances, instance)
		}
	}
	return instances, nil
}
