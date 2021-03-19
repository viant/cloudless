package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/viant/cloudless/compute/cluster"
)

func Match(criteria *cluster.Criteria) ([]cluster.Instance, error) {
	sess := session.New()
	sess.Config.Region = &criteria.Region
	svc := ec2.New(sess)

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
