package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	//galerting "github.com/grafana/grafana/pkg/services/alerting"
	"github.com/ngaut/log"
	"github.com/unrolled/render"
	"io/ioutil"
	"net/http"
	"time"
)

//AlertMsg alertmanager base struct
type AlertMsg struct {
	Receiver          string `json:"receiver"`
	Status            string `json:"status"`
	Alerts            Alerts `json:"alerts"`
	GroupLabels       KV     `json:"groupLabels"`
	CommonLabels      KV     `json:"commonLabels"`
	CommonAnnotations KV     `json:"commonAnnotations"`
	ExternalURL       string `json:"externalURL"`
}

// Alert holds one alert for notification templates.
type Alert struct {
	Status       string    `json:"status"`
	Labels       KV        `json:"labels"`
	Annotations  KV        `json:"annotations"`
	StartsAt     time.Time `json:"startsAt"`
	EndsAt       time.Time `json:"endsAt"`
	GeneratorURL string    `json:"generatorURL"`
}

// Alerts is a list of Alert objects.
type Alerts []Alert

// KV is a set of key/value string pairs.
type KV map[string]string

//AlertMsgFromWebhook get alert massage
func (r *Run) AlertMsgFromWebhook(w http.ResponseWriter, hr *http.Request) {
	b, err := ioutil.ReadAll(hr.Body)
	if err != nil {
		log.Errorf("can not read http post data with error %v", err)
		r.Rdr.Text(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Debugf("get alert data b %v", string(b))
	defer hr.Body.Close()
	alertMsg := &AlertMsg{}
	err = json.Unmarshal(b, alertMsg)
	if err != nil {
		log.Errorf("can not unmarshal http post data with error %v", err)
		r.Rdr.Text(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Debugf("get alert data %v", alertMsg)
	r.AlertMsgs <- alertMsg
	r.Rdr.Text(w, http.StatusAccepted, "")
}

//GrafanaAlertMsg grafana alert message
type GrafanaAlertMsg struct {
	Title       string `json:"title"`
	RuleID      string `json:"ruleId"`
	RuleName    string `json:"ruleName"`
	State       string `json:"state"`
	RuleURL     string `json:"ruleUrl"`
	ImageURL    string `json:"imageUrl"`
	Message     string `json:"message"`
	EvalMatches Matchs `json:"evalMatches"`
}

//Matchs all of match
type Matchs []Match

//Match grafana alert detail
type Match struct {
	Value  float64 `json:"value"`
	Metric string  `json:"metric"`
	Tags   KV      `json:"tags"`
}

//AlertMsgFormGrafana get alert message from grafana
func (r *Run) AlertMsgFormGrafana(w http.ResponseWriter, hr *http.Request) {
	b, err := ioutil.ReadAll(hr.Body)
	if err != nil {
		log.Errorf("can not read grafana http post data with error %v", err)
		r.Rdr.Text(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Debugf("get alert data grafana %v", string(b))
	defer hr.Body.Close()
	gAlertMsg := &GrafanaAlertMsg{}
	err = json.Unmarshal(b, gAlertMsg)
	if err != nil {
		log.Errorf("can not unmarshal grafana http post data with error %v", err)
		r.Rdr.Text(w, http.StatusBadRequest, err.Error())
		return
	}
	r.GrafanaAlertMsgs <- gAlertMsg
	r.Rdr.Text(w, http.StatusAccepted, "")
}

//CreateRouter create router
func (r *Run) CreateRouter() *mux.Router {
	m := mux.NewRouter()
	m.HandleFunc("/v1/alertmanager", r.AlertMsgFromWebhook).Methods("POST")
	m.HandleFunc("/v1/grafana", r.AlertMsgFromWebhook).Methods("POST")

	return m
}

// CreateRender for render.
func (r *Run) CreateRender() {
	r.Rdr = render.New(render.Options{
		IndentJSON: true,
	})
}
