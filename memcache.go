package memcache

import (
	"errors"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/gin-gonic/gin"
)

const kind = "Memcache"

var ErrCacheMiss = errors.New("memcache: cache miss")

type Item struct {
	Key        string
	Expiration time.Duration
	Value      []byte
}

type dsItem struct {
	Key       *datastore.Key `datastore:"__key__"`
	Value     []byte         `datastore:",noindex"`
	Expires   time.Time
	UpdatedAt time.Time
}

func (ds *dsItem) Load(ps []datastore.Property) error {
	return datastore.LoadStruct(ds, ps)
}

func (ds *dsItem) Save() ([]datastore.Property, error) {
	ds.UpdatedAt = time.Now()
	return datastore.SaveStruct(ds)
}

func (ds *dsItem) LoadKey(k *datastore.Key) error {
	ds.Key = k
	return nil
}

func Set(c *gin.Context, item *Item) error {
	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return err
	}

	it := toDsItem(item)

	_, err = dsClient.Put(c, newKey(item.Key), it)
	return err
}

func toDsItem(item *Item) *dsItem {
	return &dsItem{
		Value:   item.Value,
		Expires: time.Now().Add(item.Expiration),
	}
}

func Get(c *gin.Context, mkey string) (*Item, error) {
	it := new(dsItem)

	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return nil, err
	}

	err = dsClient.Get(c, newKey(mkey), it)
	if err == datastore.ErrNoSuchEntity {
		return nil, ErrCacheMiss
	}

	return toItem(it), err
}

func Delete(c *gin.Context, mkey string) error {
	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return err
	}

	err = dsClient.Delete(c, newKey(mkey))
	if err == datastore.ErrNoSuchEntity {
		return ErrCacheMiss
	}

	return err
}

func toItem(item *dsItem) *Item {
	return &Item{Value: item.Value}
}

func newKey(mkey string) *datastore.Key {
	return datastore.NameKey(kind, mkey, nil)
}
