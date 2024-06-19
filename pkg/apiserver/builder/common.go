package builder

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/spec3"

	grafanarest "github.com/grafana/grafana/pkg/apiserver/rest"
)

// TODO: this (or something like it) belongs in grafana-app-sdk,
// but lets keep it here while we iterate on a few simple examples
type APIGroupBuilder interface {
	// Get the main group name
	GetGroupVersion() schema.GroupVersion

	// Add the kinds to the server scheme
	InstallSchema(scheme *runtime.Scheme) error

	// Build the group+version behavior
	GetAPIGroupInfo(
		scheme *runtime.Scheme,
		codecs serializer.CodecFactory,
		optsGetter generic.RESTOptionsGetter,
		desiredMode grafanarest.DualWriterMode,
		reg prometheus.Registerer,
	) (*genericapiserver.APIGroupInfo, error)

	// Get OpenAPI definitions
	GetOpenAPIDefinitions() common.GetOpenAPIDefinitions

	// Get the API routes for each version
	GetAPIRoutes() *APIRoutes

	// Optionally add an authorization hook
	// Standard namespace checking will happen before this is called, specifically
	// the namespace must matches an org|stack that the user belongs to
	GetAuthorizer() authorizer.Authorizer

	// Get the desired dual writing mode. These are modes 1, 2, 3 and 4 if
	// the feature flag `unifiedStorage` is enabled and mode 0 if it is not enabled.
	// #TODO add type for map[string]grafanarest.DualWriterMode?
	GetDesiredDualWriterMode(dualWrite bool, toMode map[string]grafanarest.DualWriterMode) grafanarest.DualWriterMode
}

// Builders that implement OpenAPIPostProcessor are given a chance to modify the schema directly
type OpenAPIPostProcessor interface {
	PostProcessOpenAPI(*spec3.OpenAPI) (*spec3.OpenAPI, error)
}

// This is used to implement dynamic sub-resources like pods/x/logs
type APIRouteHandler struct {
	Path    string           // added to the appropriate level
	Spec    *spec3.PathProps // Exposed in the open api service discovery
	Handler http.HandlerFunc // when Level = resource, the resource will be available in context
}

// APIRoutes define explicit HTTP handlers in an apiserver
// TBD: is this actually necessary -- there may be more k8s native options for this
type APIRoutes struct {
	// Root handlers are registered directly after the apiVersion identifier
	Root []APIRouteHandler

	// Namespace handlers are mounted under the namespace
	Namespace []APIRouteHandler
}

type APIRegistrar interface {
	RegisterAPI(builder APIGroupBuilder)
}
