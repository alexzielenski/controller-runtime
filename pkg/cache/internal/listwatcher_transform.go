/*
Copyright 2021 The Kubernetes Authors.

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

package internal

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

type watcherWrapper struct {
	wrapped watch.Interface
	channel <-chan watch.Event
}

type ObjectTransformerFunc func(runtime.Object) runtime.Object

func newWatcherWrapper(wrapped watch.Interface, transformer ObjectTransformerFunc) *watcherWrapper {
	output := make(chan watch.Event)

	go func() {
		input := wrapped.ResultChan()
		for event := range input {
			output <- watch.Event{
				Type:   event.Type,
				Object: transformer(event.Object),
			}
		}
		close(output)
	}()

	return &watcherWrapper{
		wrapped: wrapped,
		channel: output,
	}
}

func (w *watcherWrapper) Stop() {
	w.wrapped.Stop()
}

func (w *watcherWrapper) ResultChan() <-chan watch.Event {
	return w.channel
}

func wrapListWatchWithTransformer(lw *cache.ListWatch, transformer ObjectTransformerFunc) *cache.ListWatch {
	if transformer == nil {
		return lw
	}

	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			res, err := lw.ListFunc(opts)
			if err != nil {
				return nil, err
			}

			if !meta.IsListType(res) {
				return nil, fmt.Errorf("inner List func did not return a list")
			}

			itemsList, err := meta.ExtractList(res)
			if err != nil {
				return nil, fmt.Errorf("failed to get Items of list to transform: %v", err)
			}

			for i, val := range itemsList {
				transformer(val)
				itemsList[i] = val
			}

			err = meta.SetList(res, itemsList)
			if err != nil {
				return nil, fmt.Errorf("failed to transform list: %v", err)
			}

			return res, nil
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			res, err := lw.WatchFunc(opts)
			if err != nil {
				return nil, err
			}
			return newWatcherWrapper(res, transformer), nil
		},
	}
}
