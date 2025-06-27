package uploads

import "github.com/storacha/guppy/pkg/preparation/uploads/model"

type UploadsAPI struct {
	repo Repo
}

// CreateUpload creates a new upload with the given name and options.
func (u UploadsAPI) CreateUpload(name string, options ...model.UploadOption) (*model.Upload, error) {
	return u.repo.CreateUpload(name, options...)
}
