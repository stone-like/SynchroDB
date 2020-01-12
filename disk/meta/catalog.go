package meta

import (
	"encoding/json"
	"os"
	"path/filepath"

	"io/ioutil"
	"path"
)

type Catalog struct {
	Schemes []*Scheme `json:"schemes"`
}

const (
	catalogPath = `C:\Users\user\Desktop\Catalog`
)

func NewCatalog() *Catalog {
	return &Catalog{}
}

//AddSchemeした後に永続化するかもしれないのでnewとは分けておく
func SaveCatalog(databaseName string, c *Catalog) error {
	jsonCatalog, err := json.Marshal(c)
	if err != nil {
		return err
	}

	if _, err := os.Stat(catalogPath); err != nil {
		err := os.MkdirAll(catalogPath, 0777)
		if err != nil {
			panic(err)
		}
	}
	path := filepath.Join(catalogPath, databaseName)
	err = ioutil.WriteFile(path, jsonCatalog, 0777)
	if err != nil {
		return err
	}

	return nil
}

func LoadCatalog(databaseName string) (*Catalog, error) {
	b, err := ioutil.ReadFile(path.Join(catalogPath, databaseName))
	var catalog Catalog
	err = json.Unmarshal(b, &catalog)

	if err != nil {
		return nil, err
	}
	return &catalog, err
}

func (c *Catalog) AddScheme(scheme *Scheme) {
	c.Schemes = append(c.Schemes, scheme)
}

func (c *Catalog) HasScheme(tableName string) bool {
	return c.FetchScheme(tableName) != nil
}

func (c *Catalog) FetchScheme(tableName string) *Scheme {
	for _, s := range c.Schemes {
		if s.TableName == tableName {
			return s
		}
	}
	return nil
}
