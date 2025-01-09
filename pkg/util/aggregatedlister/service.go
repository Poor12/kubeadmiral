/*
Copyright 2023 The KubeAdmiral Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aggregatedlister

import (
	"fmt"
	"strings"

	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/util/clusterobject"
	"github.com/kubewharf/kubeadmiral/pkg/util/informermanager"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type ServiceLister struct {
	federatedInformerManager informermanager.FederatedInformerManager
}

type ServiceNamespaceLister struct {
	namespace string

	federatedInformerManager informermanager.FederatedInformerManager
}

var (
	_ cache.GenericLister          = &ServiceLister{}
	_ cache.GenericNamespaceLister = &ServiceNamespaceLister{}
)

func NewServiceLister(informer informermanager.FederatedInformerManager) *ServiceLister {
	return &ServiceLister{federatedInformerManager: informer}
}

func (s *ServiceLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	clusters, err := s.federatedInformerManager.GetReadyClusters()
	if err != nil {
		return nil, err
	}
	for _, cluster := range clusters {
		serviceLister, servicesSynced, exists := s.federatedInformerManager.GetResourceListerFromFactory(common.ServiceGVR, cluster.Name)
		if !exists || !servicesSynced() {
			continue
		}

		svcLister, ok := serviceLister.(corev1listers.ServiceLister)
		if !ok {
			continue
		}

		services, err := svcLister.List(selector)
		if err != nil {
			continue
		}
		for i := range services {
			service := services[i].DeepCopy()
			clusterobject.MakeObjectUnique(service, cluster.Name)
			ret = append(ret, service)
		}
	}
	return ret, nil
}

func (s *ServiceLister) Get(name string) (runtime.Object, error) {
	items := strings.Split(name, "/")
	if len(items) != 2 {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("invalid name %q", name))
	}
	return s.ByNamespace(items[0]).Get(items[1])
}

func (s *ServiceLister) ByNamespace(namespace string) cache.GenericNamespaceLister {
	return &ServiceNamespaceLister{federatedInformerManager: s.federatedInformerManager, namespace: namespace}
}

func (s *ServiceNamespaceLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	clusters, err := s.federatedInformerManager.GetReadyClusters()
	if err != nil {
		return nil, err
	}
	for _, cluster := range clusters {
		serviceLister, servicesSynced, exists := s.federatedInformerManager.GetResourceListerFromFactory(common.ServiceGVR, cluster.Name)
		if !exists || !servicesSynced() {
			continue
		}
		svcLister, ok := serviceLister.(corev1listers.ServiceLister)
		if !ok {
			continue
		}
		svcs, err := svcLister.Services(s.namespace).List(selector)
		if err != nil {
			continue
		}
		for i := range svcs {
			svc := svcs[i].DeepCopy()
			clusterobject.MakeObjectUnique(svc, cluster.Name)
			ret = append(ret, svc)
		}
	}
	return ret, nil
}

func (s *ServiceNamespaceLister) Get(name string) (runtime.Object, error) {
	clusters, err := s.federatedInformerManager.GetReadyClusters()
	if err != nil {
		return nil, err
	}

	for _, cluster := range clusterobject.GetPossibleClusters(clusters, name) {
		serviceLister, servicesSynced, exists := s.federatedInformerManager.GetResourceListerFromFactory(common.ServiceGVR, cluster)
		if !exists || !servicesSynced() {
			continue
		}

		svcLister, ok := serviceLister.(corev1listers.ServiceLister)
		if !ok {
			continue
		}
		svcs, err := svcLister.Services(s.namespace).List(labels.Everything())
		if err != nil {
			continue
		}
		for i := range svcs {
			if name == clusterobject.GenUniqueName(cluster, svcs[i].Name) {
				svc := svcs[i].DeepCopy()
				clusterobject.MakeObjectUnique(svc, cluster)
				return svc, nil
			}
		}
	}
	return nil, apierrors.NewNotFound(corev1.Resource("service"), name)
}
