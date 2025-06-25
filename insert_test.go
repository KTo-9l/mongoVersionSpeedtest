package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type myFile struct {
	Id       string    `bson:"_id"`
	FileName string    `bson:"fileName"`
	EditDate time.Time `bson:"editDate"`
	Count    int       `bson:"count"`
	Updated  bool      `bson:"updated,omitempty"`
}

func fBenchmarkInsertOne(b *testing.B) {
	_, err := coll.InsertOne(context.TODO(), bson.M{
		"_id":      "fafa0",
		"fileName": "fakeFile.fake",
		"count":    0,
	})
	if err != nil {
		b.Error("Error InsertOne:", err)
	}
}

func fBenchmarkInsertManyThousand(b *testing.B) {
	currentTime := time.Now()

	files := []any{}

	for i := 1; i <= 1000; i++ {
		someFile := bson.M{
			"_id":      fmt.Sprintf("fafa%d", i),
			"fileName": fmt.Sprintf("fakeFile.fake%d", i),
			"editDate": currentTime,
			"count":    i,
		}
		// fmt.Println(someFile)
		files = append(files, someFile)
	}

	b.ResetTimer()

	_, err := coll.InsertMany(context.TODO(), files)
	if err != nil {
		b.Error("Error InsertManyThousand:", err)
	}
}

func fBenchmarkInsertManyMillion(b *testing.B) {
	currentTime := time.Now()

	files := []any{}

	for i := 1; i <= 1000000; i++ {
		someFile := bson.M{
			"_id":      fmt.Sprintf("fafa%d", i),
			"fileName": fmt.Sprintf("fakeFile.fake%d", i),
			"editDate": currentTime,
			"count":    i,
		}
		files = append(files, someFile)
	}

	b.ResetTimer()

	_, err := coll.InsertMany(context.TODO(), files)
	if err != nil {
		b.Error("Error InsertManyMillion:", err)
	}
}

func fBenchmarkUpdateOne(b *testing.B) {
	filter := bson.M{"count": 10}
	update := bson.M{"$set": bson.M{"updated": true}}

	_, err := coll.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		b.Error("Error UpdateOne:", err)
	}
}

func fBenchmarkUpdateMany(b *testing.B) {
	filter := bson.M{}
	update := bson.M{"$set": bson.M{"updated": true}}

	_, err := coll.UpdateMany(context.TODO(), filter, update)
	if err != nil {
		b.Error("Error UpdateMany:", err)
	}
}

func fBenchmarkDeleteOne(b *testing.B) {
	_, err := coll.DeleteOne(context.TODO(), bson.M{"updated": true})
	if err != nil {
		b.Error("Error DeleteOne:", err)
	}
}

func fBenchmarkDeleteMany(b *testing.B) {
	_, err := coll.DeleteMany(context.TODO(), bson.M{})
	if err != nil {
		b.Error("Error DeleteMany:", err)
	}
}

func fBenchmarkCollectionDrop(b *testing.B) {
	err := coll.Drop(context.TODO())
	if err != nil {
		b.Error("Error drop collection:", err)
	}
}

func fBenchmarkFindOne(b *testing.B) {
	result := coll.FindOne(context.TODO(), bson.M{})
	if result.Err() != nil {
		b.Error("Error FindOne:", result.Err())
	}
}

func fBenchmarkFindOneByIdWithoutDeserialization(b *testing.B) {
	result := coll.FindOne(context.TODO(), bson.M{"_id": "fafa5"})
	if result.Err() != nil {
		b.Error("Error FindOne:", result.Err())
	}
}

func fBenchmarkFindOneByIdWithDeserialization(b *testing.B) {
	var file myFile

	b.ResetTimer()

	err := coll.FindOne(context.TODO(), bson.M{"_id": "fafa5"}).Decode(&file)
	if err != nil {
		b.Error("Error FindOne:", err)
	}
}

func fBenchmarkFindManyUsingIndexWithoutDeserialization(b *testing.B) {
	indexModel := mongo.IndexModel{
		Keys: bson.D{{"count", 1}},
	}

	indexName, err := coll.Indexes().CreateOne(context.TODO(), indexModel)
	if err != nil {
		b.Error("Error creating index:", err)
	}

	b.ResetTimer()

	filter := bson.M{"count": bson.M{"$lt": 5}}
	opts := options.Find().SetHint(indexName)

	_, err = coll.Find(context.TODO(), filter, opts)
	if err != nil {
		b.Error("Error BenchmarkFindManyUsingIndexWithoutDeserialization:", err)
	}
}

func fBenchmarkFindManyUsingIndexWithDeserialization(b *testing.B) {
	indexModel := mongo.IndexModel{
		Keys: bson.D{{"count", 1}},
	}

	indexName, err := coll.Indexes().CreateOne(context.TODO(), indexModel)
	if err != nil {
		b.Error("Error creating index:", err)
	}

	b.ResetTimer()

	filter := bson.M{"count": bson.M{"$lt": 5}}
	opts := options.Find().SetHint(indexName)

	cursor, err := coll.Find(context.TODO(), filter, opts)
	if err != nil {
		b.Error("Error BenchmarkFindManyUsingIndexWithDeserialization:", err)
	}

	var files []myFile
	if err = cursor.All(context.TODO(), &files); err != nil {
		b.Error("Error decoding:", err)
	}
}

func fBenchmarkFindAll(b *testing.B) {
	_, err := coll.Find(context.TODO(), bson.M{})
	if err != nil {
		b.Error("Error FindAll:", err)
	}
}

func fBenchmarkGridFSInsertFromStreamThousand(b *testing.B) {
	bucket := db.GridFSBucket()

	for i := 0; i < 1000; i++ {
		file, err := os.Open("./fileForInsert.txt")
		uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"metadata tag", "first"}})
		_, err = bucket.UploadFromStream(
			context.TODO(),
			"fileForInsert.txt",
			io.Reader(file),
			uploadOpts,
		)
		if err != nil {
			b.Error(err)
		}
	}
	// fmt.Printf("New file uploaded with ID %s\n", objectID)
}

func fBenchmarkGridFSInsertFromStreamMillion(b *testing.B) {
	bucket := db.GridFSBucket()

	for i := 0; i < 1000000; i++ {
		file, err := os.Open("./fileForInsert.txt")
		uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"metadata tag", "first"}})
		_, err = bucket.UploadFromStream(
			context.TODO(),
			"fileForInsert.txt",
			io.Reader(file),
			uploadOpts,
		)
		if err != nil {
			b.Error(err)
		}
	}
	// fmt.Printf("New file uploaded with ID %s\n", objectID)
}

func fBenchmarkGridFSInsertOpenUploadStreamThousand(b *testing.B) {
	bucket := db.GridFSBucket()
	b.ResetTimer()

	for i := 0; i < 1000; i++ {
		file, err := os.Open("./fileForInsert.txt")
		if err != nil {
			b.Error(err)
		}
		// Defines options that specify configuration information for files
		// uploaded to the bucket
		uploadOpts := options.GridFSUpload().SetChunkSizeBytes(200000)
		// Writes a file to an output stream
		uploadStream, err := bucket.OpenUploadStream(context.TODO(), "fileForInsert.txt", uploadOpts)
		if err != nil {
			b.Error(err)
		}
		fileContent, err := io.ReadAll(file)
		if err != nil {
			b.Error(err)
		}
		// var bytes int
		if _, err = uploadStream.Write(fileContent); err != nil {
			b.Error(err)
		}

		// fmt.Printf("New file uploaded with %d bytes written", bytes)
		//  Calls the Close() method to write file metadata
		if err := uploadStream.Close(); err != nil {
			b.Error(err)
		}
	}
}

func fBenchmarkGridFSInsertOpenUploadStreamMillion(b *testing.B) {
	bucket := db.GridFSBucket()
	b.ResetTimer()

	for i := 0; i < 1000000; i++ {
		file, err := os.Open("./fileForInsert.txt")
		if err != nil {
			b.Error(err)
		}
		// Defines options that specify configuration information for files
		// uploaded to the bucket
		uploadOpts := options.GridFSUpload().SetChunkSizeBytes(200000)
		// Writes a file to an output stream
		uploadStream, err := bucket.OpenUploadStream(context.TODO(), "fileForInsert.txt", uploadOpts)
		if err != nil {
			b.Error(err)
		}
		fileContent, err := io.ReadAll(file)
		if err != nil {
			b.Error(err)
		}
		// var bytes int
		if _, err = uploadStream.Write(fileContent); err != nil {
			b.Error(err)
		}

		// fmt.Printf("New file uploaded with %d bytes written", bytes)
		//  Calls the Close() method to write file metadata
		if err := uploadStream.Close(); err != nil {
			b.Error(err)
		}
	}
}

func fBenchmarkGridFSSearchAndDownloadToInputStream(b *testing.B) {
	bucket := db.GridFSBucket()
	b.ResetTimer()

	fileBuffer := bytes.NewBuffer(nil)
	if _, err := bucket.DownloadToStreamByName(context.TODO(), "fileForInsert.txt", fileBuffer); err != nil {
		b.Error(err)
	}
}

func fBenchmarkGridFSSearchAndDownloadToOutputStream(b *testing.B) {
	bucket := db.GridFSBucket()
	b.ResetTimer()

	downloadStream, err := bucket.OpenDownloadStreamByName(context.TODO(), "fileForInsert.txt")
	if err != nil {
		b.Error(err)
	}
	fileBytes := make([]byte, 1024)
	if _, err := downloadStream.Read(fileBytes); err != nil {
		b.Error(err)
	}
}

func fBenchmarkDropBucket(b *testing.B) {
	bucket := db.GridFSBucket()
	b.ResetTimer()

	if err := bucket.Drop(context.TODO()); err != nil {
		panic(err)
	}
}
