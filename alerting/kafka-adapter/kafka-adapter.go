package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/unrolled/render"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	port         = flag.Int("port", 28082, "http listen port")
	kafkaAddress = flag.String("kafka-address", "10.0.3.4:9092,10.0.3.5:9092,10.0.3.6:9092", "kafka adddress")
	kafkaTopic   = flag.String("kafka-topic", "test", "kafka topic")
	logFile      = flag.String("log-file", "kafka-adapter.log", "log file")
	logLevel     = flag.String("log-level", "info", "log level: debug, info, warn, error, fatal")
)

//Run represent runtime informations
type Run struct {
	Rdr              *render.Render
	AlertMsgs        chan *AlertMsg
	GrafanaAlertMsgs chan *GrafanaAlertMsg
	KafkaClient      sarama.SyncProducer
}

func checkParams() error {
	if *kafkaAddress == "" {
		return errors.New("please input kafka address")
	}
	if *kafkaTopic == "" {
		return errors.New("please input kafka topic")
	}
	return nil
}

func initLog() error {
	log.SetLevelByString(*logLevel)
	if *logFile != "" {
		return log.SetOutputByName(*logFile)
	}
	return nil
}

//Scheduler for monitor chann data
func (r *Run) Scheduler() {
	for {
		lenAlertMsgs := len(r.AlertMsgs)
		if lenAlertMsgs > 0 {
			for i := 0; i < lenAlertMsgs; i++ {
				r.TransferMsg(<-r.AlertMsgs)
			}
		}
		lenGrafanaMsgs := len(r.GrafanaAlertMsgs)
		if lenGrafanaMsgs > 0 {
			for i := 0; i < lenGrafanaMsgs; i++ {
				r.TransferGrafanaMsg(<-r.GrafanaAlertMsgs)
			}
		}
		time.Sleep(3 * time.Second)
	}
}

func main() {
	flag.Parse()
	if err := checkParams(); err != nil {
		fmt.Printf("params error: %v", err)
		return
	}
	if err := initLog(); err != nil {
		fmt.Printf("init log file error: %v", err)
		return
	}

	r := &Run{
		AlertMsgs:        make(chan *AlertMsg, 1000),
		GrafanaAlertMsgs: make(chan *GrafanaAlertMsg, 1000),
	}
	if err := r.CreateKafkaProduce(); err != nil {
		log.Errorf("create kafka produce error %v", err)
		return
	}
	go r.Scheduler()
	go func() {
		log.Infof("create http server")
		r.CreateRender()
		http.ListenAndServe(fmt.Sprintf(":%d", *port), r.CreateRouter())
	}()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sig := <-sc
		log.Errorf("Got signal [%d] to exit.", sig)
		r.KafkaClient.Close()
		wg.Done()
	}()

	wg.Wait()

}
