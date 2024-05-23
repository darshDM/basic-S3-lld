package main

import "fmt"

type StorageStrategy interface {
	Put(key string, data []byte)
	Retrieve(key string) *S3Object
	Delete(key string)
	List() []string
}

type S3Object struct {
	Key      string
	Data     []byte
	Metadata map[string]string
}

// Two different storage classes
type StandardStorageClass struct {
	storage map[string]*S3Object
}

func NewStandardStorageClass() *StandardStorageClass {
	return &StandardStorageClass{
		storage: make(map[string]*S3Object),
	}
}

type InfrequentAcessStorageClass struct {
	storage map[string]*S3Object
}

func NewInfrequentAccessStorageClass() *InfrequentAcessStorageClass {
	return &InfrequentAcessStorageClass{
		storage: make(map[string]*S3Object),
	}
}

// general struct to refer storage strategy
type StorageService struct {
	storageStrategy StorageStrategy
}

func NewStorageService(strategy StorageStrategy) *StorageService {
	return &StorageService{
		storageStrategy: strategy,
	}
}

// functions based on StandardStorageClass
func (ssc *StandardStorageClass) Put(key string, data []byte) {
	object := &S3Object{
		Key:  key,
		Data: data,
	}
	// Add Logic to store data in
	fmt.Printf(">%v : stored in Standard S3 bucket\n", key)
	ssc.storage[key] = object
}

func (ssc *StandardStorageClass) Retrieve(key string) *S3Object {
	object := ssc.storage[key]
	return object
}

func (ssc *StandardStorageClass) Delete(key string) {
	// Add logic to flush data, after that remove from map
	delete(ssc.storage, key)
}

func (ssc *StandardStorageClass) List() []string {
	keys := make([]string, 0, len(ssc.storage))
	for key := range ssc.storage {
		keys = append(keys, key)
	}
	return keys

}

func (iasc *InfrequentAcessStorageClass) Put(key string, data []byte) {
	object := &S3Object{
		Key:  key,
		Data: data,
	}
	// Add Logic to store data in
	fmt.Printf(">%v : stored in Infrequent Access S3 bucket\n", key)
	iasc.storage[key] = object
}

func (iasc *InfrequentAcessStorageClass) Retrieve(key string) *S3Object {
	object := iasc.storage[key]
	return object
}

func (iasc *InfrequentAcessStorageClass) Delete(key string) {
	// Add logic to flush data, after that remove from map
	delete(iasc.storage, key)
}

func (ssc *InfrequentAcessStorageClass) List() []string {
	keys := make([]string, 0, len(ssc.storage))
	for key := range ssc.storage {
		keys = append(keys, key)
	}
	return keys
}

// Bucket Struct
type Bucket struct {
	name           string
	storageService *StorageService
}

// Bucket Related functions
func NewBucket(name string, storageService *StorageService) *Bucket {
	return &Bucket{
		name:           name,
		storageService: storageService,
	}
}

func (b *Bucket) UploadObject(key string, data []byte) {
	b.storageService.storageStrategy.Put(key, data)
}

func (b *Bucket) DownloadObject(key string) *S3Object {
	return b.storageService.storageStrategy.Retrieve(key)
}

func (b *Bucket) DeleteObject(key string) {
	b.storageService.storageStrategy.Delete(key)
}

func (b *Bucket) ListObjects() []string {
	return b.storageService.storageStrategy.List()
}

func main() {
	storageService := NewStorageService(NewInfrequentAccessStorageClass())
	bucket := NewBucket("my-s3", storageService)

	bucket.UploadObject("my-key", []byte("my-value"))
	bucket.UploadObject("username", []byte("test-user-1"))

	fmt.Printf("Objects: \n%v\n", bucket.ListObjects())

	s3Object := bucket.DownloadObject("username")
	fmt.Printf("Object data for %v : %v\n", "username", string(s3Object.Data))
	bucket.DeleteObject("my-key")
	fmt.Printf("After removing one object: \n%v", bucket.ListObjects())
}
