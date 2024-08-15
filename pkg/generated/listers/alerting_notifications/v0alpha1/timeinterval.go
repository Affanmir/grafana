// SPDX-License-Identifier: AGPL-3.0-only

// Code generated by lister-gen. DO NOT EDIT.

package v0alpha1

import (
	v0alpha1 "github.com/grafana/grafana/pkg/apis/alerting_notifications/v0alpha1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/listers"
	"k8s.io/client-go/tools/cache"
)

// TimeIntervalLister helps list TimeIntervals.
// All objects returned here must be treated as read-only.
type TimeIntervalLister interface {
	// List lists all TimeIntervals in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v0alpha1.TimeInterval, err error)
	// TimeIntervals returns an object that can list and get TimeIntervals.
	TimeIntervals(namespace string) TimeIntervalNamespaceLister
	TimeIntervalListerExpansion
}

// timeIntervalLister implements the TimeIntervalLister interface.
type timeIntervalLister struct {
	listers.ResourceIndexer[*v0alpha1.TimeInterval]
}

// NewTimeIntervalLister returns a new TimeIntervalLister.
func NewTimeIntervalLister(indexer cache.Indexer) TimeIntervalLister {
	return &timeIntervalLister{listers.New[*v0alpha1.TimeInterval](indexer, v0alpha1.Resource("timeinterval"))}
}

// TimeIntervals returns an object that can list and get TimeIntervals.
func (s *timeIntervalLister) TimeIntervals(namespace string) TimeIntervalNamespaceLister {
	return timeIntervalNamespaceLister{listers.NewNamespaced[*v0alpha1.TimeInterval](s.ResourceIndexer, namespace)}
}

// TimeIntervalNamespaceLister helps list and get TimeIntervals.
// All objects returned here must be treated as read-only.
type TimeIntervalNamespaceLister interface {
	// List lists all TimeIntervals in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v0alpha1.TimeInterval, err error)
	// Get retrieves the TimeInterval from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v0alpha1.TimeInterval, error)
	TimeIntervalNamespaceListerExpansion
}

// timeIntervalNamespaceLister implements the TimeIntervalNamespaceLister
// interface.
type timeIntervalNamespaceLister struct {
	listers.ResourceIndexer[*v0alpha1.TimeInterval]
}
