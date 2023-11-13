package association

import (
	"context"
	"errors"

	dataloader "github.com/graph-gophers/dataloader/v7"
)

var ErrNotFound = errors.New("not found")

type (
	QueryFunc[K comparable, V any] func(ctx context.Context, keys []K) ([]V, error)
	KeyFunc[K comparable, V any]   func(v V) K

	BatchOptions[V any] struct {
		sortFn func([]V)
	}

	BatchOption[V any] func(*BatchOptions[V])
)

func NewHasOneBatchFunc[K comparable, V any](queryFn QueryFunc[K, V], keyFn KeyFunc[K, V]) dataloader.BatchFunc[K, V] {
	return func(ctx context.Context, keys []K) []*dataloader.Result[V] {
		rows, err := queryFn(ctx, keys)
		if err != nil {
			return newErrorResults[V](err, len(keys))
		}

		data := make(map[K]V, len(rows))
		for _, v := range rows {
			key := keyFn(v)
			data[key] = v
		}

		return newResults(keys, data)
	}
}

func NewHasManyBatchFunc[K comparable, V any, S []V](queryFn QueryFunc[K, V], keyFn KeyFunc[K, V], optionFns ...BatchOption[V]) dataloader.BatchFunc[K, S] {
	opts := BatchOptions[V]{}
	for _, optionFn := range optionFns {
		optionFn(&opts)
	}

	return func(ctx context.Context, keys []K) []*dataloader.Result[S] {
		rows, err := queryFn(ctx, keys)
		if err != nil {
			return newErrorResults[S](err, len(keys))
		}

		data := make(map[K]S, len(rows))
		for _, v := range rows {
			key := keyFn(v)
			if _, ok := data[key]; !ok {
				data[key] = make(S, 0)
			}

			data[key] = append(data[key], v)
		}

		if opts.sortFn != nil {
			for _, v := range data {
				opts.sortFn(v)
			}
		}

		return newResults(keys, data)
	}
}

func NewManyToManyBatchFunc[K comparable, A any, V any, S []V](
	associationQueryFn QueryFunc[K, A],
	queryFn QueryFunc[K, V],
	parentKeyFn KeyFunc[K, A],
	childKeyFn KeyFunc[K, A],
	keyFn KeyFunc[K, V],
	optionFns ...BatchOption[V],
) dataloader.BatchFunc[K, S] {
	opts := BatchOptions[V]{}
	for _, optionFn := range optionFns {
		optionFn(&opts)
	}

	return func(ctx context.Context, keys []K) []*dataloader.Result[S] {
		associations, err := associationQueryFn(ctx, keys)
		if err != nil {
			return newErrorResults[S](err, len(keys))
		}

		childKeys := make([]K, len(associations))
		for i, ass := range associations {
			childKey := childKeyFn(ass)
			childKeys[i] = childKey
		}

		rows, err := queryFn(ctx, childKeys)
		if err != nil {
			return newErrorResults[S](err, len(keys))
		}

		values := make(map[K]V, len(rows))
		for _, v := range rows {
			key := keyFn(v)
			values[key] = v
		}

		data := make(map[K]S, len(rows))
		for _, ass := range associations {
			childKey := childKeyFn(ass)
			parentKey := parentKeyFn(ass)
			if _, ok := data[parentKey]; !ok {
				data[parentKey] = make(S, 0)
			}

			val, ok := values[childKey]
			if ok {
				data[parentKey] = append(data[parentKey], val)
			}
		}

		if opts.sortFn != nil {
			for _, v := range data {
				opts.sortFn(v)
			}
		}

		return newResults(keys, data)
	}
}

func WithSortFunc[V any](sortFn func([]V)) BatchOption[V] {
	return func(opts *BatchOptions[V]) {
		opts.sortFn = sortFn
	}
}

func newResults[K comparable, V any](keys []K, data map[K]V) []*dataloader.Result[V] {
	results := make([]*dataloader.Result[V], len(keys))
	for i, key := range keys {
		results[i] = new(dataloader.Result[V])
		v, ok := data[key]
		if ok {
			results[i].Data = v
		} else {
			results[i].Error = ErrNotFound
		}
	}
	return results
}

func newErrorResults[V any](err error, n int) []*dataloader.Result[V] {
	results := make([]*dataloader.Result[V], n)
	for i := range results {
		results[i] = &dataloader.Result[V]{Error: err}
	}
	return results
}
