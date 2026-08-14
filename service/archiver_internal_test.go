package service

import (
	"testing"

	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
)

// Конструктор архивера ходит в B2, поэтому для проверки чистой логики сервис
// собирается напрямую — на эти методы сеть не влияет.
func bareArchiver(name string) *ArchiverService {
	return &ArchiverService{
		db:        testDataBase,
		tableName: "axq_" + name,
		logger:    zerolog.Nop(),
		packRatio: 1,
	}
}

// C6: позиция архивера хранится в id сообщений, а fid в его счётчике — это
// номер блоба в B2, а не в базе. Стартовый DB-fid приходится искать по id.
func TestArchiver_ReaderStartFID_FindsBlobByMessageID(t *testing.T) {
	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.MaxBlobSize = 10
	w := newWriter(t, wopts)
	pushN(t, w, 100)

	blobs := blobsOf(t, name)
	a := bareArchiver(name)

	for _, target := range []domain.Blob{blobs[0], blobs[3], blobs[len(blobs)-1]} {
		got, err := a.readerStartFID(target.ToId)
		if err != nil {
			t.Fatalf("readerStartFID(%d): %v", target.ToId, err)
		}
		if got != target.FID {
			t.Fatalf("для lastId=%d получили fid=%d, ожидали %d", target.ToId, got, target.FID)
		}
	}
}

// C6b: архивер не должен начинать с нуля — иначе он перечитывает всю таблицу.
func TestArchiver_ReaderStartFID_NotZeroOnRestart(t *testing.T) {
	name := testQueue(t)
	wopts := writerOpts(t, name)
	wopts.MaxBlobSize = 10
	w := newWriter(t, wopts)
	pushN(t, w, 100)

	a := bareArchiver(name)
	got, err := a.readerStartFID(55)
	if err != nil {
		t.Fatalf("readerStartFID: %v", err)
	}
	if got <= 1 {
		t.Fatalf("для lastId=55 стартовый fid=%d — таблица будет перечитана с начала", got)
	}
}

// C6c: позиция архивера указывает внутрь уже удалённого блоба — продолжать
// надо с первого сохранившегося, а не ждать вырезанный fid.
func TestArchiver_ReaderStartFID_HeadDeleted(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 100)

	blobs := blobsOf(t, name)
	deadTo := blobs[4]
	firstAlive := blobs[5]
	if err := testDataBase.Table("axq_"+name).Where("fid <= ?", deadTo.FID).Delete(&domain.Blob{}).Error; err != nil {
		t.Fatalf("удаление головы: %v", err)
	}

	a := bareArchiver(name)
	got, err := a.readerStartFID(blobs[1].ToId) // id из уже удалённого блоба
	if err != nil {
		t.Fatalf("readerStartFID: %v", err)
	}
	if got != firstAlive.FID {
		t.Fatalf("получили fid=%d, ожидали первый живой %d", got, firstAlive.FID)
	}
}

// C6c2: не осталось ни одного блоба, который дотягивал бы до позиции архивера.
// Тогда работает запасная ветка — самый старый сохранившийся блоб.
func TestArchiver_ReaderStartFID_NothingReachesPosition(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 100)

	blobs := blobsOf(t, name)
	// сносим хвост: теперь ни один блоб не покрывает id=100
	if err := testDataBase.Table("axq_"+name).Where("fid >= ?", blobs[50].FID).Delete(&domain.Blob{}).Error; err != nil {
		t.Fatalf("удаление хвоста: %v", err)
	}

	a := bareArchiver(name)
	got, err := a.readerStartFID(100)
	if err != nil {
		t.Fatalf("readerStartFID: %v", err)
	}
	if got != blobs[0].FID {
		t.Fatalf("получили fid=%d, ожидали самый старый сохранившийся %d", got, blobs[0].FID)
	}
}

// C6d: пустая таблица — нулевая позиция, ридер просто подождёт райтера.
func TestArchiver_ReaderStartFID_EmptyTable(t *testing.T) {
	name := testQueue(t)
	newWriter(t, writerOpts(t, name))

	a := bareArchiver(name)
	got, err := a.readerStartFID(0)
	if err != nil {
		t.Fatalf("readerStartFID на пустой таблице: %v", err)
	}
	if got != 0 {
		t.Fatalf("на пустой таблице получили fid=%d, ожидали 0", got)
	}
}

func fillArchiverBlob(a *ArchiverService, count, msgSize int) {
	a.messageList = &protobuf.BlobMessageList{Fid: 1}
	a.rawSize = 0
	payload := make([]byte, msgSize)
	for i := 0; i < count; i++ {
		m := &protobuf.BlobMessage{Id: uint64(i + 1), Message: payload}
		a.messageList.Messages = append(a.messageList.Messages, m)
		a.rawSize += int64(proto.Size(m))
	}
}

// C7: упакованный блоб обязан разбираться обратно — это тот же путь, которым
// его потом читает b2reader.
func TestArchiver_PackBlob_RoundTrip(t *testing.T) {
	key := []byte("12345678901234567890123456789012")
	a := bareArchiver("unused")
	a.opts.B2.Compression = domain.CompressionOptions{
		Compression:   domain.BLOB_COMPRESSION_GZIP,
		Encryption:    domain.BLOB_ENCRYPTION_AES,
		EncryptionKey: key,
	}
	a.b2Aes = utils.NewAES(key)
	fillArchiverBlob(a, 100, 64)

	packed, err := a.packBlob()
	if err != nil {
		t.Fatalf("packBlob: %v", err)
	}

	raw, err := a.b2Aes.Decrypt(packed)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	raw, err = utils.GUnzipData(raw)
	if err != nil {
		t.Fatalf("GUnzipData: %v", err)
	}
	var got protobuf.BlobMessageList
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Messages) != 100 {
		t.Fatalf("после распаковки %d сообщений, упаковывали 100", len(got.Messages))
	}
}

// C7b: packBlob обязан уточнять packRatio по факту — на этом держится вся
// экономия, ради которой убрана перепаковка на каждый ChunkSize.
func TestArchiver_PackBlob_UpdatesRatio(t *testing.T) {
	a := bareArchiver("unused")
	a.opts.B2.Compression = domain.CompressionOptions{Compression: domain.BLOB_COMPRESSION_GZIP}
	fillArchiverBlob(a, 500, 128) // сжимаемые нули

	packed, err := a.packBlob()
	if err != nil {
		t.Fatalf("packBlob: %v", err)
	}
	if a.packRatio >= 1 {
		t.Fatalf("packRatio=%f после gzip сжимаемых данных, ожидали заметно меньше 1", a.packRatio)
	}
	want := float64(len(packed)) / float64(a.rawSize)
	if a.packRatio != want {
		t.Fatalf("packRatio=%f, ожидали %f", a.packRatio, want)
	}
}

// C7c: главный регресс на утечку. После неудачной проверки оценка обязана
// сойтись с фактом, иначе readyToPack срабатывал бы на каждом следующем
// сообщении и блоб перепаковывался бы снова и снова — ровно то поведение,
// которое съедало память.
func TestArchiver_ReadyToPack_DoesNotThrash(t *testing.T) {
	a := bareArchiver("unused")
	a.opts.B2.Compression = domain.CompressionOptions{Compression: domain.BLOB_COMPRESSION_GZIP}
	a.opts.MaxCount = 1_000_000
	a.opts.MaxSize = 1 << 20 // 1 МБ упакованных
	fillArchiverBlob(a, 5000, 256)

	if !a.readyToPack() {
		t.Fatal("при packRatio=1 и сыром размере больше MaxSize проверка должна была сработать")
	}
	packed, err := a.packBlob()
	if err != nil {
		t.Fatalf("packBlob: %v", err)
	}
	if len(packed) > a.opts.MaxSize {
		t.Skip("данные не сжались ниже MaxSize — для этой проверки нужен блоб поменьше")
	}

	// оценка сошлась с фактом, значит следующего сообщения недостаточно,
	// чтобы снова потребовать упаковку
	if a.readyToPack() {
		t.Fatal("readyToPack сработал повторно сразу после упаковки — блоб будет перепакован на каждом сообщении")
	}
}

// C7d: MaxCount остаётся жёстким потолком независимо от оценки размера.
func TestArchiver_ReadyToPack_MaxCountIsHardLimit(t *testing.T) {
	a := bareArchiver("unused")
	a.opts.MaxCount = 100
	a.opts.MaxSize = 1 << 30 // заведомо недостижимо по размеру
	a.packRatio = 0.0001     // оценка говорит «ещё далеко»
	fillArchiverBlob(a, 100, 8)

	if !a.readyToPack() {
		t.Fatal("достигнут MaxCount, но упаковка не запрошена")
	}
}
