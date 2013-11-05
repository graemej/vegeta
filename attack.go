package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	vegeta "github.com/tsenart/vegeta/lib"
	"log"
	"net/http"
	"strings"
	"time"
)

func attackCmd(args []string) command {
	fs := flag.NewFlagSet("attack", flag.ExitOnError)
	protocol := fs.String("protocol", "http", "Protocol used for the attack (http|kafka)")
	rate := fs.Uint64("rate", 50, "Requests per second")
	targetsf := fs.String("targets", "stdin", "Targets file")
	ordering := fs.String("ordering", "random", "Attack ordering [sequential, random]")
	duration := fs.Duration("duration", 10*time.Second, "Duration of the test")
	output := fs.String("output", "stdout", "Output file")
	hdrs := headers{Header: make(http.Header)}
	fs.Var(hdrs, "header", "Targets request header")
	fs.Parse(args)

	if len(args) == 0 || args[0] == "help" {
		return func() error {
			fs.PrintDefaults()
			return errors.New("Help requested")
		}
	}

	switch *protocol {
	case "http":
	case "kafka":
		break
	default:
		return func() error {
			fs.PrintDefaults()
			return fmt.Errorf("Invalid protocol: %s", protocol)
		}
	}

	return func() error {
		return attack(*rate, *duration, *targetsf, *ordering, *output, hdrs.Header, *protocol)
	}
}

// attack validates the attack arguments, sets up the
// required resources, launches the attack and writes the results
func attack(rate uint64, duration time.Duration, targetsf, ordering, output string, header http.Header, protocol string) error {
	if rate == 0 {
		return fmt.Errorf(errRatePrefix + "can't be zero")
	}

	if duration == 0 {
		return fmt.Errorf(errDurationPrefix + "can't be zero")
	}

	in, err := file(targetsf, false)
	if err != nil {
		return fmt.Errorf(errTargetsFilePrefix+"(%s): %s", targetsf, err)
	}
	defer in.Close()

	var httpTargets vegeta.Targets
	var kafkaTargets vegeta.KafkaTargets

	switch protocol {
	case "http":
		httpTargets, err = vegeta.NewTargetsFrom(in)
		if err != nil {
			return fmt.Errorf(errTargetsFilePrefix+"(%s): %s", targetsf, err)
		}
		httpTargets.SetHeader(header)

	case "kafka":
		kafkaTargets, err = vegeta.NewKakfaTargetsFrom(in)
		if err != nil {
			return fmt.Errorf(errTargetsFilePrefix+"(%s): %s", targetsf, err)
		}
	}

	switch ordering {
	case "random":
		httpTargets.Shuffle(time.Now().UnixNano())
		kafkaTargets.Shuffle(time.Now().UnixNano())
	case "sequential":
		break
	default:
		return fmt.Errorf(errOrderingPrefix+"`%s` is invalid", ordering)
	}

	out, err := file(output, true)
	if err != nil {
		return fmt.Errorf(errOutputFilePrefix+"(%s): %s", output, err)
	}
	defer out.Close()

	var results vegeta.Results
	switch protocol {
	case "http":
		log.Printf("Vegeta is attacking %d targets in %s order for %s...\n", len(httpTargets), ordering, duration)
		results = vegeta.Attack(httpTargets, rate, duration)
	case "kafka":
		log.Printf("Vegeta is attacking %d targets in %s order for %s...\n", len(kafkaTargets), ordering, duration)
		results = vegeta.KafkaAttack(kafkaTargets, rate, duration)
	}

	log.Println("Done!")
	log.Printf("Writing results to '%s'...", output)
	if err := results.Encode(out); err != nil {
		return err
	}
	return nil
}

const (
	errRatePrefix        = "Rate: "
	errDurationPrefix    = "Duration: "
	errOutputFilePrefix  = "Output file: "
	errTargetsFilePrefix = "Targets file: "
	errOrderingPrefix    = "Ordering: "
	errReportingPrefix   = "Reporting: "
)

// headers is the http.Header used in each target request
// it is defined here to implement the flag.Value interface
// in order to support multiple identical flags for request header
// specification
type headers struct{ http.Header }

func (h headers) String() string {
	buf := &bytes.Buffer{}
	if err := h.Write(buf); err != nil {
		return ""
	}
	return buf.String()
}

func (h headers) Set(value string) error {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return fmt.Errorf("Header '%s' has a wrong format", value)
	}
	key, val := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	if key == "" || val == "" {
		return fmt.Errorf("Header '%s' has a wrong format", value)
	}
	h.Add(key, val)
	return nil
}
