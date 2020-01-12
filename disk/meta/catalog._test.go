package meta

import (
	"testing"
)

func TestSaveAndLoadCatalog(t *testing.T) {
	catalog := NewCatalog()
	aSlice := []string{"id", "name"}
	bSlice := []ColType{Int, VarChar}
	scheme := NewScheme("tempTable", aSlice, bSlice)
	catalog.AddScheme(scheme)
	SaveCatalog("tempDatabase", catalog)

	newcatalog, err := LoadCatalog("tempDatabase")
	if err != nil {
		t.Error("error")
	}
	if !newcatalog.HasScheme("tempTable") {
		t.Error("error")
	}

}
