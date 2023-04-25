package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"time"
)

func HealthyHostCount(loadBalancerName, region string) (float64, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return 0, err
	}

	cw := cloudwatch.New(sess)
	if cw == nil {
		return 0, err
	}

	awsPeriod := int64(60)
	request := cloudwatch.GetMetricStatisticsInput{
		StartTime:  aws.Time(time.Now().UTC().Add(time.Second * -60)),
		EndTime:    aws.Time(time.Now().UTC()),
		MetricName: aws.String("HealthyHostCount"),
		Period:     &awsPeriod,
		Statistics: []*string{aws.String("Average")},
		Namespace:  aws.String("AWS/ELB"),
		Unit:       aws.String("Count"),
		Dimensions: []*cloudwatch.Dimension{{Name: aws.String("LoadBalancerName"), Value: &loadBalancerName}},
	}
	resp, err := cw.GetMetricStatistics(&request)
	if err != nil {
		return 0, err
	}
	if len(resp.Datapoints) > 0 {
		return *resp.Datapoints[0].Average, nil
	}
	return 0, nil
}
