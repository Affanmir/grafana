package legacy_storage

import "github.com/grafana/grafana/pkg/apimachinery/errutil"

var (
	ErrNoAlertmanagerConfiguration  = errutil.Internal("alerting.notification.configMissing", errutil.WithPublicMessage("No alertmanager configuration present in this organization"))
	ErrBadAlertmanagerConfiguration = errutil.Internal("alerting.notification.configCorrupted").MustTemplate("Failed to unmarshal the Alertmanager configuration", errutil.WithPublic("Current Alertmanager configuration in the storage is corrupted. Reset the configuration or rollback to a recent valid one."))

	ErrReceiverNotFound = errutil.NotFound("alerting.notifications.receiver.notFound")
	ErrReceiverExists   = errutil.BadRequest("alerting.notifications.receiver.exists", errutil.WithPublicMessage("Receiver with this name already exists. Use a different name or update an existing one."))
)

func makeErrBadAlertmanagerConfiguration(err error) error {
	data := errutil.TemplateData{
		Public: map[string]interface{}{
			"Error": err.Error(),
		},
		Error: err,
	}
	return ErrBadAlertmanagerConfiguration.Build(data)
}
