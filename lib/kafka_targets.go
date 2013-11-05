package vegeta

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"io"
	"math/rand"
	"net/url"
	"strings"
)

type KafkaTarget struct {
	action   string           // MESSAGE / SAFEMESSAGE
	url      *url.URL         // server:port/topic/partition
	producer *sarama.Producer // producer
	payload  string           // json payload
}

// Targets represents the http.Requests which will be issued during the test
type KafkaTargets []*KafkaTarget

// NewKakfaTargetsFrom reads targets out of a line separated source skipping empty lines
func NewKakfaTargetsFrom(source io.Reader) (KafkaTargets, error) {
	scanner := bufio.NewScanner(source)
	lines := make([]string, 0)
	for scanner.Scan() {
		line := scanner.Text()

		if line = strings.TrimSpace(line); line != "" && line[0:2] != "//" {
			// Skipping comments or blank lines
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return KafkaTargets{}, err
	}

	return NewKafkaTargets(lines)
}

// NewTargets instantiates Targets from a slice of strings
func NewKafkaTargets(lines []string) (KafkaTargets, error) {
	clients := make(map[string]*sarama.Client)
	producers := make(map[*url.URL]*sarama.Producer)

	targets := make([]*KafkaTarget, 0)
	for _, line := range lines {
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return targets, fmt.Errorf("Invalid request format: `%s`", line)
		}

		// Parse URL
		url, err := url.ParseRequestURI(parts[1])
		if err != nil {
			return targets, fmt.Errorf("Failed to parse URL: %s", err)
		}

		// Create connection
		if clients[url.Host] == nil {
			fmt.Printf("Creating Sarama client for %s\n", url.Host)
			client, err := sarama.NewClient("vegeta", []string{url.Host}, nil)
			if err != nil {
				return targets, fmt.Errorf("Failed to create Kafka client: %s", err)
			}
			clients[url.Host] = client
		}

		if producers[url] == nil {
			fmt.Printf("Creating Sarama producer for %s%s\n", url.Host, url.Path)
			client := clients[url.Host]
			producer, err := sarama.NewProducer(client, "vegeta", nil)
			if err != nil {
				return targets, fmt.Errorf("Failed to create Kafka producer: %s", err)
			}
			producers[url] = producer
		}

		target := &KafkaTarget{parts[0], url, producers[url], parts[2]}
		fmt.Printf("%s %s %s\n", target.action, target.url, target.payload)
		targets = append(targets, target)
	}
	return targets, nil
}

// Shuffle randomly alters the order of Targets with the provided seed
func (t KafkaTargets) Shuffle(seed int64) {
	rand.Seed(seed)
	for i, rnd := range rand.Perm(len(t)) {
		t[i], t[rnd] = t[rnd], t[i]
	}
}
