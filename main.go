// Main is the only package for Taweret
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/kanisterio/kanister/pkg/apis/cr/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type backup struct {
	name, schedule, status, backupLocation string
	time                                   time.Time
	inUse                                  bool
}

type backupconfig struct {
	Name              string `yaml:"name"`
	KanisterNamespace string `yaml:"kanisterNamespace"`
	BlueprintName     string `yaml:"blueprintName"`
	ProfileName       string `yaml:"profileName"`
	Retention         struct {
		Backups StringInt `yaml:"backups"`
		Minutes StringInt `yaml:"minutes"`
		Hours   StringInt `yaml:"hours"`
		Days    StringInt `yaml:"days"`
		Months  StringInt `yaml:"months"`
		Years   StringInt `yaml:"years"`
	}
}

// StringInt is a type for custom YAML unmarshalling
type StringInt int

type taweretmetrics struct {
	backupCount  *prometheus.GaugeVec
	oldestBackup *prometheus.GaugeVec
	newestBackup *prometheus.GaugeVec
}

type backupcounts struct {
	pending  int
	running  int
	failed   int
	skipped  int
	deleting int
}

func main() {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// initialise dynamicClient
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// create the clientSet
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// specify the crds which should be queried
	gvr := schema.GroupVersionResource{
		Group:    "cr.kanister.io",
		Version:  "v1alpha1",
		Resource: "actionsets",
	}

	taweretMetrics := initialiseMetrics()

	scheduleEvaluations(dynamicClient, gvr, clientSet, taweretMetrics)

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}

func scheduleEvaluations(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, clientSet *kubernetes.Clientset, taweretMetrics taweretmetrics) {
	// set evaluation schedule
	const evalSchedule string = "*/10 * * * *"

	// schedule backup evaluations
	s := gocron.NewScheduler(time.UTC)
	job, err := s.Cron(evalSchedule).Do(startEvaluation, dynamicClient, gvr, clientSet, taweretMetrics)
	if err != nil {
		log.Fatalf("error creating job: %v", err)
	}
	s.StartAsync()
	log.Printf("first evaluation scheduled: %v, evaluation schedule: %v", job.NextRun(), evalSchedule)

}

func startEvaluation(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, clientSet *kubernetes.Clientset, taweretMetrics taweretmetrics) {
	log.Printf("starting backup config evaluations\n")

	// get backupConfigs
	backupConfigs := getBackupConfigs(clientSet, gvr)

	// evaluate backupConfigs
	for _, backupConfig := range backupConfigs {
		evaluateBackups(dynamicClient, gvr, taweretMetrics, backupConfig)
	}
	log.Printf("backup config evaluations complete\n---\n")
}

func evaluateBackups(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, taweretMetrics taweretmetrics, backupConfig backupconfig) {

	log.Printf("%v: evaluating backups\n", backupConfig.Name)

	backups := getBackups(dynamicClient, gvr, backupConfig)

	categorisedBackups, backupCounts := categoriseBackups(backups, backupConfig)

	// if there are excess daily backups, delete the oldest excess, then refetch and recategorise the backups
	if len(categorisedBackups) > int(backupConfig.Retention.Backups) {
		deleteOldestBackups(categorisedBackups, (len(categorisedBackups) - int(backupConfig.Retention.Backups)), dynamicClient, gvr, backupConfig)
		backups = getBackups(dynamicClient, gvr, backupConfig)
		categorisedBackups, backupCounts = categoriseBackups(backups, backupConfig)
	} else {
		log.Printf("%v: no backups deleted: current: %v limit: %v\n", backupConfig.Name, len(categorisedBackups), backupConfig.Retention.Backups)
	}

	taweretMetrics.setMetrics(categorisedBackups, backupConfig, backupCounts)

	log.Printf("%v: backup evaluation complete\n", backupConfig.Name)
}

func getBackupConfigs(clientset *kubernetes.Clientset, gvr schema.GroupVersionResource) []backupconfig {
	var backupConfigs []backupconfig
	// get configmaps
	configmaps, err := clientset.CoreV1().ConfigMaps("kanister").List(context.TODO(), v1.ListOptions{})
	if err != nil {
		log.Printf("error getting actionsets: %v\n", err)
		os.Exit(1)
	}

	for _, configmap := range configmaps.Items {
		if configmap.Data["backup-config.yaml"] != "" {
			var backupConfig backupconfig

			err = yaml.Unmarshal([]byte(configmap.Data["backup-config.yaml"]), &backupConfig)
			if err != nil {
				log.Printf("error unmarshalling backup-config.yaml: %v\n", err)
				os.Exit(1)
			}

			backupConfigs = append(backupConfigs, backupConfig)

			log.Printf("backup config:\n name: %v\n kanister namespace: %v\n blueprint name: %v\n profile name: %v\n retention:\n backups: %v\n years: %v months: %v days: %v hours %v minutes: %v", backupConfig.Name, backupConfig.KanisterNamespace, backupConfig.BlueprintName, backupConfig.ProfileName, backupConfig.Retention.Backups, backupConfig.Retention.Years, backupConfig.Retention.Months, backupConfig.Retention.Days, backupConfig.Retention.Hours, backupConfig.Retention.Minutes)
		}
	}
	return backupConfigs
}

// queries Kubernetes for Actionsets, adds the actionsets with action name 'backup' to a slice of backup objects and returns the slice
// func getBackups(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, backupConfig backupconfig) []backup {
// 	var backups []backup

// 	log.Printf("%v: retrieving actionsets from Kubernetes", backupConfig.Name)

// 	// get actionsets
// 	actionsets, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).List(context.Background(), v1.ListOptions{})
// 	if err != nil {
// 		log.Printf("%v: error getting actionsets: %v\n", backupConfig.Name, err)
// 		os.Exit(1)
// 	}

// 	log.Printf("%v: filtering backup actionsets from Kubernetes", backupConfig.Name)

// 	// loop through actionsets
// 	for _, actionset := range actionsets.Items {
// 		actionSpec := actionset.Object["spec"].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})
// 		actionMetadata := actionset.Object["metadata"].(map[string]interface{})

// 		// skip ahead if the ActionSet is not a backup
// 		if actionSpec["name"] != "backup" {
// 			continue
// 		}

// 		// check for the existence of the keys, if they do not exist, return early. The if statements are split up to avoid runtime errors.
// 		if _, ok := actionSpec["options"]; !ok {
// 			continue
// 		}
// 		if _, ok := actionSpec["options"].(map[string]interface{})["backup-schedule"]; !ok {
// 			continue
// 		}
// 		if _, ok := actionset.Object["status"].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})["artifacts"].(map[string]interface{})["cloudObject"].(map[string]interface{})["keyValue"].(map[string]interface{})["backupLocation"]; !ok {
// 			continue
// 		}

// 		thisBackup := backup{
// 			name:           fmt.Sprintf("%v", actionMetadata["name"]),
// 			status:         fmt.Sprintf("%v", actionset.Object["status"].(map[string]interface{})["state"]),
// 			schedule:       fmt.Sprintf("%v", actionSpec["options"].(map[string]interface{})["backup-schedule"]),
// 			backupLocation: fmt.Sprintf("%v", actionset.Object["status"].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})["artifacts"].(map[string]interface{})["cloudObject"].(map[string]interface{})["keyValue"].(map[string]interface{})["backupLocation"]),
// 		}
// 		thisBackup.time, _ = time.Parse(time.RFC3339, fmt.Sprintf("%v", actionMetadata["creationTimestamp"]))
// 		if thisBackup.schedule == backupConfig.Name {
// 			backups = append(backups, thisBackup)
// 		}
// 	}
// 	return backups
// }

// queries Kubernetes for Actionsets, adds the actionsets with action name 'backup' to a slice of backup objects and returns the slice
func getBackups(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, backupConfig backupconfig) []backup {
    var backups []backup

    log.Printf("%v: retrieving actionsets from Kubernetes", backupConfig.Name)

    // get actionsets
    actionsets, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).List(context.Background(), v1.ListOptions{})
    if err != nil {
        log.Printf("%v: error getting actionsets: %v\n", backupConfig.Name, err)
        os.Exit(1)
    }

    log.Printf("%v: filtering backup actionsets from Kubernetes", backupConfig.Name)

    // loop through actionsets
    for _, actionset := range actionsets.Items {
        actionSpec, ok := actionset.Object["spec"].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})
        if !ok {
            continue
        }
        actionMetadata, ok := actionset.Object["metadata"].(map[string]interface{})
        if !ok {
            continue
        }

        // skip ahead if the ActionSet does not start with "backup"
        if !strings.HasPrefix(actionSpec["name"].(string), "backup") {
            continue
        }

        // check for the existence of the keys, if they do not exist, return early. The if statements are split up to avoid runtime errors.
        options, ok := actionSpec["options"].(map[string]interface{})
        if !ok {
            continue
        }
        backupSchedule, ok := options["backup-schedule"].(string)
        if !ok {
            continue
        }

        var backupLocation string
        if artifacts, ok := actionset.Object["status"].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})["artifacts"].(map[string]interface{}); ok {
            if cloudObject, ok := artifacts["cloudObject"].(map[string]interface{}); ok {
                backupLocation, _ = cloudObject["backupLocation"].(string)
				if !ok {
                    backupLocation = ""
                }
			}
        }

        thisBackup := backup{
            name:           fmt.Sprintf("%v", actionMetadata["name"]),
            status:         fmt.Sprintf("%v", actionset.Object["status"].(map[string]interface{})["state"]),
            schedule:       backupSchedule,
            // backupLocation: backupLocation,
        }
        if backupLocation != "" {
            thisBackup.backupLocation = backupLocation
        }
		thisBackup.time, _ = time.Parse(time.RFC3339, fmt.Sprintf("%v", actionMetadata["creationTimestamp"]))
        if thisBackup.schedule == backupConfig.Name {
            log.Printf("Selected actionset: %v", thisBackup.name)
            backups = append(backups, thisBackup)
        }
    }
    return backups
}

// determine whether individual backups are required based on max retention dates and their category (daily, weekly, none)
func categoriseBackups(uncategorisedBackups []backup, backupConfig backupconfig) ([]backup, backupcounts) {
	var categorisedBackups []backup
	backupCounts := backupcounts{
		pending:  0,
		running:  0,
		failed:   0,
		skipped:  0,
		deleting: 0,
	}

	log.Printf("%v: categorising backups\n", backupConfig.Name)

	maxBackupDateTime := time.Now()

	maxBackupDateTime = maxBackupDateTime.Add(time.Minute * time.Duration(backupConfig.Retention.Minutes) * -1)
	maxBackupDateTime = maxBackupDateTime.Add(time.Hour * time.Duration(backupConfig.Retention.Hours) * -1)
	maxBackupDateTime = maxBackupDateTime.AddDate(int(backupConfig.Retention.Years)*-1, int(backupConfig.Retention.Months)*-1, int(backupConfig.Retention.Days)*-1)

	for _, aBackup := range uncategorisedBackups {
		if aBackup.time.After(maxBackupDateTime) && (aBackup.status == "complete" || aBackup.status == "failed") {
			aBackup.inUse = true
			categorisedBackups = append(categorisedBackups, aBackup)
		} else if aBackup.status == "pending" {
			backupCounts.pending++
		} else if aBackup.status == "running" {
			backupCounts.running++
		} else if aBackup.status == "failed" || aBackup.status == "attemptfailed" {
			backupCounts.failed++
		} else if aBackup.status == "skipped" {
			backupCounts.skipped++
		} else if aBackup.status == "deleting" {
			backupCounts.deleting++
		}
	}

	categorisedAndSortedBackups := sortBackups(categorisedBackups, backupConfig)
	log.Printf("%v: categorised backups: %v\n", backupConfig.Name, len(categorisedAndSortedBackups))
	return categorisedAndSortedBackups, backupCounts
}

// delete a specified number of the oldest backups in a backup slice
func deleteOldestBackups(backups []backup, count int, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, backupConfig backupconfig) {
	backups = sortBackups(backups, backupConfig)
	for i := 0; i < count; i++ {
		log.Printf("%v: deleting backup %v, backup time: %v, deletion nr %v, total to delete %v, total backups in category: %v\n", backupConfig.Name, backups[i].name, backups[i].time.UTC(), i+1, count, len(backups))
		deleteBackup(backups[i], dynamicClient, gvr, backupConfig)
	}
}

// sort the backup slices with the oldest backups placed at the start of the slice
func sortBackups(backups []backup, backupConfig backupconfig) []backup {
	log.Printf("%v: sorting backups chronologically\n", backupConfig.Name)
	sort.Slice(backups, func(q, p int) bool {
		return backups[p].time.After(backups[q].time)
	})
	return backups
}

// deletes a specified backup by creating an actionset with the action 'delete'
// func deleteBackup(unusedBackup backup, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, backupConfig backupconfig) {

// 	// set name of deletion actionset
// 	deletionActionsetName := fmt.Sprintf("delete-%v", unusedBackup.name)

// 	// check if the deletion actionset already exists
//     _, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Get(context.Background(), deletionActionsetName, v1.GetOptions{})
//     if err == nil {
//         log.Printf("Deletion actionset %v already exists, skipping creation", deletionActionsetName)
//         return
//     }
// 	// construct actionset crd manifest to delete backup
// 	deletionActionSet := v1alpha1.ActionSet{
// 		Spec: &v1alpha1.ActionSetSpec{
// 			Actions: []v1alpha1.ActionSpec{
// 				{
// 					Name:      "delete",
// 					Blueprint: backupConfig.BlueprintName,
// 					Artifacts: map[string]v1alpha1.Artifact{
// 						"cloudObject": {
// 							KeyValue: map[string]string{
// 								"backupLocation": unusedBackup.backupLocation,
// 							},
// 						},
// 					},
// 					Object: v1alpha1.ObjectReference{
// 						Kind:      "namespace",
// 						Name:      backupConfig.KanisterNamespace,
// 						Namespace: backupConfig.KanisterNamespace,
// 					},
// 					Profile: &v1alpha1.ObjectReference{
// 						Name:      backupConfig.ProfileName,
// 						Namespace: backupConfig.KanisterNamespace,
// 					},
// 				},
// 			},
// 		},
// 		TypeMeta: v1.TypeMeta{
// 			APIVersion: "cr.kanister.io/v1alpha1",
// 			Kind:       "ActionSet",
// 		},
// 		ObjectMeta: v1.ObjectMeta{
// 			Name:      deletionActionsetName,
// 			Namespace: backupConfig.KanisterNamespace,
// 		},
// 	}

// 	// convert to unstructured to apply with dynamicClient
// 	myCRAsUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&deletionActionSet)
// 	if err != nil {
// 		panic(err.Error())
// 	}
// 	myCRUnstructured := &unstructured.Unstructured{Object: myCRAsUnstructured}

// 	// apply deletion actionset
// 	appliedActionSet, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Create(context.Background(), myCRUnstructured, v1.CreateOptions{})
// 	log.Printf("Applying the following deletion actionset: %v", appliedActionSet)
// 	if err != nil {
// 		panic(err.Error())
// 	}

// 	// loop to check status of deletion actionset whilst actionset is running
// 	for {
// 		log.Printf("%v: waiting for %v to complete... ", backupConfig.Name, deletionActionsetName)
// 		time.Sleep(5 * time.Second)

// 		// get deletion actionset
// 		actionset, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Get(context.Background(), deletionActionsetName, v1.GetOptions{})
// 		if err != nil {
// 			log.Printf("%v: error retrieving deletion actionset: %v\n", backupConfig.Name, err)
// 			os.Exit(1)
// 		}

// 		// check if deletion actionset status is "complete"
// 		if actionset.Object["status"].(map[string]interface{})["state"] == "complete" {
// 			log.Printf("%v: %v has completed\n", backupConfig.Name, deletionActionsetName)
// 			break
// 		}

// 		// check if deletion actionset status is "failed"
// 		if actionset.Object["status"].(map[string]interface{})["state"] == "failed" {
// 			log.Printf("%v: error deleting backup with actionset %v, error: %v\n", backupConfig.Name, deletionActionsetName, actionset.Object["status"].(map[string]interface{})["error"].(map[string]interface{})["message"])
// 			break
// 		}

// 		// print current state of deletion actionset
// 		log.Printf("%v\n", actionset.Object["status"].(map[string]interface{})["state"])
// 	}

// 	// delete backup actionset
// 	err = dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Delete(context.Background(), unusedBackup.name, v1.DeleteOptions{})
// 	if err != nil {
// 		log.Printf("%v: error deleting backup actionset: %v\n", backupConfig.Name, err)
// 		os.Exit(1)
// 	}

// }

// deletes a specified backup by creating an actionset with the action 'delete'
func deleteBackup(unusedBackup backup, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, backupConfig backupconfig) {
    // set name of deletion actionset
    deletionActionsetName := fmt.Sprintf("delete-%v", unusedBackup.name)

    // check if the deletion actionset already exists
    _, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Get(context.Background(), deletionActionsetName, v1.GetOptions{})
    if err == nil {
        log.Printf("Deletion actionset %v already exists, skipping creation", deletionActionsetName)
        return
    }

    // construct actionset crd manifest to delete backup
    deletionActionSet := v1alpha1.ActionSet{
        Spec: &v1alpha1.ActionSetSpec{
            Actions: []v1alpha1.ActionSpec{
                {
                    Name:      "delete",
                    Blueprint: backupConfig.BlueprintName,
                    Object: v1alpha1.ObjectReference{
                        Kind:      "namespace",
                        Name:      backupConfig.KanisterNamespace,
                        Namespace: backupConfig.KanisterNamespace,
                    },
                },
            },
        },
        TypeMeta: v1.TypeMeta{
            APIVersion: "cr.kanister.io/v1alpha1",
            Kind:       "ActionSet",
        },
        ObjectMeta: v1.ObjectMeta{
            Name:      deletionActionsetName,
            Namespace: backupConfig.KanisterNamespace,
        },
    }

    // Add Artifacts if backupLocation exists
    if unusedBackup.backupLocation != "" {
        deletionActionSet.Spec.Actions[0].Artifacts = map[string]v1alpha1.Artifact{
            "cloudObject": {
                KeyValue: map[string]string{
                    "backupLocation": unusedBackup.backupLocation,
                },
            },
        }
    }

    // Add Profile if provided
    if backupConfig.ProfileName != "" {
        deletionActionSet.Spec.Actions[0].Profile = &v1alpha1.ObjectReference{
            Name:      backupConfig.ProfileName,
            Namespace: backupConfig.KanisterNamespace,
        }
    }

    // convert to unstructured to apply with dynamicClient
    myCRAsUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&deletionActionSet)
    if err != nil {
        panic(err.Error())
    }
    myCRUnstructured := &unstructured.Unstructured{Object: myCRAsUnstructured}

    // apply deletion actionset
    appliedActionSet, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Create(context.Background(), myCRUnstructured, v1.CreateOptions{})
    log.Printf("Applying the following deletion actionset: %v", appliedActionSet)
    if err != nil {
        panic(err.Error())
    }

    // loop to check status of deletion actionset whilst actionset is running
    for {
        log.Printf("%v: waiting for %v to complete... ", backupConfig.Name, deletionActionsetName)
        time.Sleep(5 * time.Second)

        // get deletion actionset
        actionset, err := dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Get(context.Background(), deletionActionsetName, v1.GetOptions{})
        if err != nil {
            log.Printf("%v: error retrieving deletion actionset: %v\n", backupConfig.Name, err)
            os.Exit(1)
        }

        // check if deletion actionset status is "complete"
        if actionset.Object["status"].(map[string]interface{})["state"] == "complete" {
            log.Printf("%v: %v has completed\n", backupConfig.Name, deletionActionsetName)
            break
        }

        // check if deletion actionset status is "failed"
        if actionset.Object["status"].(map[string]interface{})["state"] == "failed" {
            log.Printf("%v: error deleting backup with actionset %v, error: %v\n", backupConfig.Name, deletionActionsetName, actionset.Object["status"].(map[string]interface{})["error"].(map[string]interface{})["message"])
            break
        }

        // print current state of deletion actionset
        log.Printf("%v\n", actionset.Object["status"].(map[string]interface{})["state"])
    }

    // delete backup actionset
    err = dynamicClient.Resource(gvr).Namespace(backupConfig.KanisterNamespace).Delete(context.Background(), unusedBackup.name, v1.DeleteOptions{})
    if err != nil {
        log.Printf("%v: error deleting backup actionset: %v\n", backupConfig.Name, err)
        os.Exit(1)
    }
}
// UnmarshalYAML is a custom YAML unmarshaller to allow string to stringint type conversion
func (st *StringInt) UnmarshalYAML(b []byte) error {
	var item interface{}
	if err := yaml.Unmarshal(b, &item); err != nil {
		return err
	}
	switch v := item.(type) {
	case int:
		*st = StringInt(v)
	case float64:
		*st = StringInt(int(v))
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		*st = StringInt(i)
	}
	return nil
}

// initialise Prometheus metrics
func initialiseMetrics() taweretmetrics {
	var taweretMetrics taweretmetrics
	taweretMetrics.backupCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backup_count",
			Help: "The amount of backups",
		},
		[]string{
			// which backup config
			"backup_config_name",
			// state of the backups
			"backup_status",
		},
	)
	taweretMetrics.oldestBackup = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "oldest_backup_timestamp",
			Help: "The amount of backups",
		},
		[]string{
			// which backup config
			"backup_config_name",
		},
	)
	taweretMetrics.newestBackup = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "newest_backup_timestamp",
			Help: "The amount of backups",
		},
		[]string{
			// which backup config
			"backup_config_name",
		},
	)

	prometheus.MustRegister(taweretMetrics.backupCount)
	prometheus.MustRegister(taweretMetrics.oldestBackup)
	prometheus.MustRegister(taweretMetrics.newestBackup)

	return taweretMetrics
}

// set Prometheus metrics values
func (taweretMetrics *taweretmetrics) setMetrics(backups []backup, backupConfig backupconfig, backupCounts backupcounts) {
	log.Printf("%v: setting Prometheus metrics\n", backupConfig.Name)

	// set newestBackup and oldestBackup to corresponding backup timestamps if backups are present
	if len(backups) > 0 {
		taweretMetrics.oldestBackup.WithLabelValues(backupConfig.Name).Set(float64(backups[0].time.Unix()))
		taweretMetrics.newestBackup.WithLabelValues(backupConfig.Name).Set(float64(backups[len(backups)-1].time.Unix()))

	} else {
		taweretMetrics.oldestBackup.WithLabelValues(backupConfig.Name).Set(0)
		taweretMetrics.newestBackup.WithLabelValues(backupConfig.Name).Set(0)
	}

	// set backupCount for completed, pending, running, failed, skipped and deleting state backups
	taweretMetrics.backupCount.WithLabelValues(backupConfig.Name, "completed").Set(float64(len(backups)))
	taweretMetrics.backupCount.WithLabelValues(backupConfig.Name, "pending").Set(float64(backupCounts.pending))
	taweretMetrics.backupCount.WithLabelValues(backupConfig.Name, "running").Set(float64(backupCounts.running))
	taweretMetrics.backupCount.WithLabelValues(backupConfig.Name, "failed").Set(float64(backupCounts.failed))
	taweretMetrics.backupCount.WithLabelValues(backupConfig.Name, "skipped").Set(float64(backupCounts.skipped))
	taweretMetrics.backupCount.WithLabelValues(backupConfig.Name, "deleting").Set(float64(backupCounts.deleting))
}
