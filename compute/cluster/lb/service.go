package lb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
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

func New() *Service {
	return &Service{}
}
