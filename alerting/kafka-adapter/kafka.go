package main

import (
	//"fmt"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"strconv"
	"strings"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

//CreateKafkaProduce create a kafka produce
func (r *Run) CreateKafkaProduce() error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	var err error
	r.KafkaClient, err = sarama.NewSyncProducer(strings.Split(*kafkaAddress, ","), config)
	return err
}

//PushKafkaMsg push message to kafka cluster
func (r *Run) PushKafkaMsg(msg string) error {
	kafkaMsg := &sarama.ProducerMessage{
		Topic: *kafkaTopic,
		Value: sarama.StringEncoder(msg),
	}
	log.Debugf("get kafka mssage %s", msg)
	_, _, err := r.KafkaClient.SendMessage(kafkaMsg)
	return err
}

//KafkaMsg represent kafka msg
type KafkaMsg struct {
	Title       string `json:"title"`
	Source      string `json:"source"`
	Node        string `json:"node"`
	Expr        string `json:"expr"`
	Description string `json:"description"`
	URL         string `json:"url"`
	Level       string `json:"level"`
	Note        string `json:"note"`
	Value       string `json:"value"`
	Time        string `json:"time"`
}

//TransferMsg transfer alert to kafka string
func (r *Run) TransferMsg(am *AlertMsg) {
	for _, at := range am.Alerts {
		kafkaMsg := &KafkaMsg{
			Title:       getValue(at.Labels, "alertname"),
			Description: getValue(at.Annotations, "description"),
			Expr:        getValue(at.Labels, "expr"),
			Level:       getValue(at.Labels, "level"),
			Node:        getValue(at.Labels, "instance"),
			Source:      getValue(at.Labels, "env"),
			Value:       getValue(at.Annotations, "value"),
			Note:        getValue(at.Annotations, "summary"),
			URL:         at.GeneratorURL,
			Time:        at.StartsAt.Format(timeFormat),
		}
		atByte, err := json.Marshal(kafkaMsg)
		if err != nil {
			log.Errorf("can not marshal data with error %v", err)
			continue
		}

		if err := r.PushKafkaMsg(string(atByte)); err != nil {
			log.Errorf("push message to kafka error %v", err)
		}
	}
}

func getValue(kv KV, key string) string {
	if val, ok := kv[key]; ok {
		return val
	}
	return ""
}

//GrafanaKafkaMsg grafana messge to kafka
type GrafanaKafkaMsg struct {
	Title    string           `json:"title"`
	Match    []SendKafkaMatch `json:"match"`
	Message  string           `json:"message"`
	URL      string           `json:"url"`
	ImageURL string           `json:"image_url"`
	Status   string           `json:"status"`
	Note     string           `json:"note"`
}

//SendKafkaMatch grafana match instance
type SendKafkaMatch struct {
	Instance string `json:"instance"`
	Value    string `json:"string"`
}

//TransferGrafanaMsg grafana message to kafka
func (r *Run) TransferGrafanaMsg(gam *GrafanaAlertMsg) {
	skm := []SendKafkaMatch{}
	for _, evalM := range gam.EvalMatches {
		m := SendKafkaMatch{
			Instance: getValue(evalM.Tags, "instance"),
			Value:    strconv.FormatFloat(evalM.Value, 'f', 9, 64),
		}
		skm = append(skm, m)
	}
	grafanaMsg := &GrafanaKafkaMsg{
		Title:    gam.Title,
		Message:  gam.Message,
		URL:      gam.RuleURL,
		ImageURL: gam.ImageURL,
		Status:   gam.State,
		Match:    skm,
	}

	atByte, err := json.Marshal(grafanaMsg)
	if err != nil {
		log.Errorf("can not marshal grafanaMsg data with error %v", err)
		return
	}

	if err := r.PushKafkaMsg(string(atByte)); err != nil {
		log.Errorf("push message to grafana mesg error %v", err)
	}
}
