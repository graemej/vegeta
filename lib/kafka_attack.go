package vegeta

import (
	"github.com/Shopify/sarama"
	"time"
)

// Attack hits the passed KafkaTargets at the rate specified for
// duration time and then waits for all the requests to come back.
// The results of the attack are put into a slice which is returned.
func KafkaAttack(targets KafkaTargets, rate uint64, duration time.Duration) Results {
	total := rate * uint64(duration.Seconds())
	hits := make(chan *KafkaTarget, total)
	res := make(chan Result, total)
	results := make(Results, total)
	// Scatter
	go func() {
		throttle := time.Tick(time.Duration(1e9 / rate))
		for req := range hits {
			<-throttle
			go hitKakfa(req, res)
		}
	}()

	for i := 0; i < cap(hits); i++ {
		hits <- targets[i%len(targets)]
	}
	close(hits)
	// Gather
	for i := 0; i < cap(res); i++ {
		results[i] = <-res
	}
	close(res)

	return results.Sort()
}

// hit executes the passed http.Request and puts the result into results.
// Both transport errors and unsucessfull requests (non {2xx,3xx}) are
// considered errors.
func hitKakfa(target *KafkaTarget, res chan Result) {
	began := time.Now()
	target.producer.SendMessage(nil, sarama.StringEncoder(target.payload))
	var err error
	err = nil
	//	r, err := client.Do(req)
	result := Result{
		Timestamp: began,
		Latency:   time.Since(began),
		BytesOut:  uint64(len(target.payload)),
	}
	if err != nil {
		result.Error = err.Error()
	} else {
		result.BytesIn = 0
	}
	res <- result
}
