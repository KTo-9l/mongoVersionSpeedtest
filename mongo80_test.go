package main

import (
	"context"
	"testing"
)

func BenchmarkMongo80(b *testing.B) {
	client, err := connectMongo80()
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	println("Connected to Mongo80")

	coll = client.Database("benchmarkMain").Collection("files")
	b.ResetTimer()

	// 1. вставка одной записи (Insert)
	// b.Run("InsertOne:", fBenchmarkInsertOne)

	// 2. вставка многих (InsertMany)
	// b.Run("InsertManyThousand:", fBenchmarkInsertManyThousand)
	b.Run("InsertManyMillion:", fBenchmarkInsertManyMillion)

	// 3. обновление (Update)
	b.Run("UpdateOne:", fBenchmarkUpdateOne)
	// b.Run("UpdateMany:", fBenchmarkUpdateMany)

	// 4. Удаление (Delete)
	b.Run("DeleteOne:", fBenchmarkDeleteOne)
	// b.Run("DeleteMany:", fBenchmarkDeleteMany)

	// 5. Поиск по Id FindId без/с десериализацией
	b.Run("FindOneByIdWithoutDeserialization :", fBenchmarkFindOneByIdWithoutDeserialization)
	b.Run("FindOneByIdWithDeserialization:", fBenchmarkFindOneByIdWithDeserialization)

	// 6. Поиск нескольких с использованием индекса (Find) без/с десериализацией
	b.Run("FindManyUsingIndexWithoutDeserialization :", fBenchmarkFindManyUsingIndexWithoutDeserialization)
	b.Run("FindManyUsingIndexWithDeserialization:", fBenchmarkFindManyUsingIndexWithDeserialization)

	// b.Run("FindOne:", fBenchmarkFindOne)
	// b.Run("FindAll:", fBenchmarkFindAll)

	b.Run("DeleteAll:", fBenchmarkCollectionDrop)

	db = client.Database("benchmarkGridFS")

	// 7. GrinFS вставка
	b.Run("GridFS Upload from stream:", fBenchmarkGridFSInsertFromStreamThousand)
	// b.Run("GridFS Upload from stream:", fBenchmarkGridFSInsertFromStreamMillion) // didn't work, too long for go test

	b.Run("GridFS Upload Opening upload stream:", fBenchmarkGridFSInsertOpenUploadStreamThousand)
	// b.Run("GridFS Upload Opening upload stream:", fBenchmarkGridFSInsertOpenUploadStreamMillion) // didn't work, too long for go test

	// 8. GridFS поиск и загрузки из БД
	b.Run("GridFS Search & Download to InputStream:", fBenchmarkGridFSSearchAndDownloadToInputStream)
	b.Run("GridFS Search & Download to OutputStream:", fBenchmarkGridFSSearchAndDownloadToOutputStream)

	// b.Run("DeleteAll:", fBenchmarkDeleteMany)
	b.Run("Clear GridFS DB:", fBenchmarkDropBucket)
}
