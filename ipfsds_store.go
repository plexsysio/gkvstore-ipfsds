package ipfsdsStore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/plexsysio/gkvstore"
)

type ipfsds struct {
	ds datastore.Batching
}

func New(ds datastore.Batching) gkvstore.Store {
	return &ipfsds{ds: ds}
}

func (ds *ipfsds) Close() error {
	return nil
}

func createKey(i gkvstore.Item) datastore.Key {
	k := fmt.Sprintf(
		"%s/%s/%s",
		i.GetNamespace(),
		"k",
		i.GetID(),
	)
	return datastore.NewKey(k)
}

func createdIdxKey(i gkvstore.Item) datastore.Key {
	k := fmt.Sprintf(
		"%s/%s/%d/%s",
		i.GetNamespace(),
		"c",
		i.(gkvstore.TimeTracker).GetCreated(),
		i.GetID(),
	)
	return datastore.NewKey(k)

}

func updatedIdxKey(i gkvstore.Item) datastore.Key {
	k := fmt.Sprintf(
		"%s/%s/%d/%s",
		i.GetNamespace(),
		"u",
		i.(gkvstore.TimeTracker).GetUpdated(),
		i.GetID(),
	)
	return datastore.NewKey(k)
}

func (d *ipfsds) Create(ctx context.Context, item gkvstore.Item) error {
	batch, err := d.ds.Batch(ctx)
	if err != nil {
		return err
	}

	if setter, ok := item.(gkvstore.IDSetter); ok {
		setter.SetID(uuid.New().String())
	}

	key := createKey(item)

	if exists, _ := d.ds.Has(ctx, key); exists {
		return errors.New("key already exists")
	}

	if timeTracker, ok := item.(gkvstore.TimeTracker); ok {
		timeNow := time.Now().UnixNano()

		timeTracker.SetCreated(timeNow)
		timeTracker.SetUpdated(timeNow)

		err := batch.Put(ctx, createdIdxKey(item), nil)
		if err != nil {
			return err
		}

		err = batch.Put(ctx, updatedIdxKey(item), nil)
		if err != nil {
			return err
		}
	}

	val, err := item.Marshal()
	if err != nil {
		return err
	}

	err = batch.Put(ctx, key, val)
	if err != nil {
		return err
	}

	return batch.Commit(ctx)
}

func (d *ipfsds) Read(ctx context.Context, item gkvstore.Item) error {
	key := createKey(item)

	val, err := d.ds.Get(ctx, key)
	if err != nil {
		return gkvstore.ErrRecordNotFound
	}

	return item.Unmarshal(val)
}

func (d *ipfsds) Update(ctx context.Context, item gkvstore.Item) error {
	batch, err := d.ds.Batch(ctx)
	if err != nil {
		return err
	}

	key := createKey(item)

	if timeTracker, ok := item.(gkvstore.TimeTracker); ok {
		oldIndex := updatedIdxKey(item)

		timeTracker.SetUpdated(time.Now().UnixNano())

		err = batch.Put(ctx, updatedIdxKey(item), nil)
		if err != nil {
			return err
		}
		err = batch.Delete(ctx, oldIndex)
		if err != nil {
			return err
		}
	}

	val, err := item.Marshal()
	if err != nil {
		return err
	}

	err = batch.Put(ctx, key, val)
	if err != nil {
		return err
	}

	return batch.Commit(ctx)
}

func (d *ipfsds) Delete(ctx context.Context, item gkvstore.Item) error {
	batch, err := d.ds.Batch(ctx)
	if err != nil {
		return err
	}

	key := createKey(item)

	if _, ok := item.(gkvstore.TimeTracker); ok {
		err = batch.Delete(ctx, createdIdxKey(item))
		if err != nil {
			return err
		}
		err = batch.Delete(ctx, updatedIdxKey(item))
		if err != nil {
			return err
		}
	}

	err = batch.Delete(ctx, key)
	if err != nil {
		return err
	}

	return batch.Commit(ctx)
}

func (d *ipfsds) List(
	ctx context.Context,
	factory gkvstore.Factory,
	opt gkvstore.ListOpt,
) (<-chan *gkvstore.Result, error) {

	if _, ok := factory().(gkvstore.TimeTracker); !ok && opt.Sort != gkvstore.SortNatural {
		return nil, errors.New("indexing not supported on this item")
	}

	res := make(chan *gkvstore.Result)

	q := query.Query{
		Offset: int(opt.Limit * opt.Page),
	}

	sendIndexResults := func(results query.Results) {
		defer close(res)

		var count int64 = 0

		for v := range results.Next() {
			if v.Error != nil {
				res <- &gkvstore.Result{Err: v.Error}
				return
			}

			it := factory()
			itemKey := v.Entry.Key[strings.LastIndex(v.Entry.Key, "/")+1:]
			itemKey = it.GetNamespace() + "/k/" + itemKey
			val, err := d.ds.Get(ctx, datastore.NewKey(itemKey))
			if err != nil {
				res <- &gkvstore.Result{Err: err}
				return
			}

			err = it.Unmarshal(val)
			if err != nil {
				res <- &gkvstore.Result{Err: err}
				return
			}

			if opt.Filter != nil && opt.Filter.Compare(it) {
				continue
			}

			res <- &gkvstore.Result{Val: it}
			count++
			if count >= opt.Limit {
				return
			}
		}
	}

	switch opt.Sort {
	case gkvstore.SortNatural:
		q.Prefix = factory().GetNamespace() + "/k"
		if opt.Filter != nil {
			q.Filters = []query.Filter{userFilter{filter: opt.Filter, factory: factory}}
		}
		q.Limit = int(opt.Limit)

		results, err := d.ds.Query(ctx, q)
		if err != nil {
			return nil, err
		}
		go func() {
			defer close(res)

			for v := range results.Next() {
				if v.Error != nil {
					res <- &gkvstore.Result{Err: v.Error}
					return
				}
				it := factory()
				err := it.Unmarshal(v.Entry.Value)
				if err != nil {
					res <- &gkvstore.Result{Err: err}
					return
				}
				res <- &gkvstore.Result{Val: it}
			}
		}()
	case gkvstore.SortCreatedAsc:
		q.Prefix = factory().GetNamespace() + "/c"
		q.Orders = []query.Order{query.OrderByKey{}}
		q.KeysOnly = true

		results, err := d.ds.Query(ctx, q)
		if err != nil {
			return nil, err
		}

		go sendIndexResults(results)
	case gkvstore.SortCreatedDesc:
		q.Prefix = factory().GetNamespace() + "/c"
		q.Orders = []query.Order{query.OrderByKeyDescending{}}
		q.KeysOnly = true

		results, err := d.ds.Query(ctx, q)
		if err != nil {
			return nil, err
		}

		go sendIndexResults(results)
	case gkvstore.SortUpdatedAsc:
		q.Prefix = factory().GetNamespace() + "/u"
		q.Orders = []query.Order{query.OrderByKey{}}
		q.KeysOnly = true

		results, err := d.ds.Query(ctx, q)
		if err != nil {
			return nil, err
		}

		go sendIndexResults(results)
	case gkvstore.SortUpdatedDesc:
		q.Prefix = factory().GetNamespace() + "/u"
		q.Orders = []query.Order{query.OrderByKeyDescending{}}
		q.KeysOnly = true

		results, err := d.ds.Query(ctx, q)
		if err != nil {
			return nil, err
		}

		go sendIndexResults(results)
	}

	return res, nil
}

type userFilter struct {
	filter  gkvstore.ItemFilter
	factory gkvstore.Factory
}

func (f userFilter) Filter(e query.Entry) bool {
	s := f.factory()
	err := s.Unmarshal(e.Value)
	if err != nil {
		return false

	}
	return f.filter.Compare(s)
}
