package lb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/elb"
	"time"
)

type Service struct{}

func (s *Service) cloudWatchService(region string) (*cloudwatch.CloudWatch, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, err
	}
	cw := cloudwatch.New(sess)
	if cw == nil {
		return nil, err
	}
	return cw, nil
}

func (s *Service) CountNodes(requests ...*NodeCountRequest) (NodeCountResponses, error) {
	var responses = make([]*NodeCountResponse, 0)
	for _, request := range requests {
		nodeCountInRegion := 0
		for _, loadBalancerName := range request.LoadBalancerNames {
			nodeCount, err := s.getLoadBalancerMetrics(request, loadBalancerName)
			if err != nil {
				return nil, err
			}
			nodeCountInRegion += nodeCount
		}
		responses = append(responses, &NodeCountResponse{Region: request.Region, Count: nodeCountInRegion})
	}
	return responses, nil
}

func (s *Service) getLoadBalancerMetrics(request *NodeCountRequest, loadBalancerName string) (int, error) {
	service, err := s.cloudWatchService(request.Region)
	if err != nil {
		return 0, err
	}
	awsPeriod := int64(60)
	input := cloudwatch.GetMetricStatisticsInput{
		StartTime:  aws.Time(time.Now().UTC().Add(time.Second * -60)),
		EndTime:    aws.Time(time.Now().UTC()),
		MetricName: aws.String("HealthyHostCount"),
		Period:     &awsPeriod,
		Statistics: []*string{aws.String("Average")},
		Namespace:  aws.String("AWS/ELB"),
		Unit:       aws.String("Count"),
		Dimensions: []*cloudwatch.Dimension{{Name: aws.String("LoadBalancerName"), Value: &loadBalancerName}},
	}
	resp, err := service.GetMetricStatistics(&input)
	if err != nil {
		return 0, err
	}
	if len(resp.Datapoints) > 0 {
		return int(*resp.Datapoints[0].Average), nil
	}
	return 0, nil
}

func (s *Service) ElbIPList(requests ...*IPListRequest) (IPListResponses, error) {
	responses := make([]*IPListResponse, 0)
	for _, request := range requests {
		sess, err := session.NewSession(&aws.Config{Region: aws.String(request.Region)})
		if err != nil {
			return nil, err
		}
		loadBalancers, err := getLoadBalancers(elb.New(sess), request.LoadBalancerNames)
		if err != nil {
			return nil, err
		}
		for _, loadBalancer := range loadBalancers {
			response := &IPListResponse{
				Region:           request.Region,
				LoadBalancerName: *loadBalancer.LoadBalancerName,
			}
			instanceNames := getInstanceNames(loadBalancer.Instances)
			if len(instanceNames) == 0 {
				continue
			}
			ips, err := getIPList(ec2.New(sess), instanceNames)
			if err != nil {
				return nil, err
			}
			response.IPList = ips
			responses = append(responses, response)
		}
	}
	return responses, nil
}

func getIPList(ec2Client *ec2.EC2, instanceNames []*string) ([]string, error) {
	instanceReq := &ec2.DescribeInstancesInput{
		InstanceIds: instanceNames,
	}
	instanceResp, err := ec2Client.DescribeInstances(instanceReq)
	if err != nil {
		return nil, err
	}
	ips := make([]string, 0)
	for _, res := range instanceResp.Reservations {
		for _, instance := range res.Instances {
			ips = append(ips, *instance.PrivateIpAddress)
		}
	}
	return ips, nil
}

func getLoadBalancers(lb *elb.ELB, loadBalanceNames []*string) ([]*elb.LoadBalancerDescription, error) {
	result, err := lb.DescribeLoadBalancers(&elb.DescribeLoadBalancersInput{
		LoadBalancerNames: loadBalanceNames,
	})
	if err != nil {
		return nil, err
	}
	return result.LoadBalancerDescriptions, nil
}

func getInstanceNames(instances []*elb.Instance) []*string {
	instanceNames := make([]*string, len(instances))
	for i, instance := range instances {
		instanceNames[i] = instance.InstanceId
	}
	return instanceNames
}

func New() *Service {
	return &Service{}
}
