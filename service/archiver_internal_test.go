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

// setCounter кладёт строку потребителя очереди с заданной позицией в таблице.
func setCounter(t *testing.T, queue, reader string, fid, id uint64) {
	t.Helper()
	err := testDataBase.Save(&domain.BlobCounter{
		ReaderName: reader,
		Name:       queue,
		Fid:        fid,
		ID:         id,
	}).Error
	if err != nil {
		t.Fatalf("запись счётчика %s: %v", reader, err)
	}
}

func cleanArchiver(t *testing.T, name string, gap uint64) *ArchiverService {
	t.Helper()
	a := bareArchiver(name)
	a.ctx = testCtx(t)
	a.opts.Name = name
	a.opts.CleanGapFID = gap
	a.opts.CleanBatch = 100
	return a
}

func remainingFIDs(t *testing.T, name string) []uint64 {
	t.Helper()
	var fids []uint64
	if err := testDataBase.Table("axq_"+name).Order("fid asc").Pluck("fid", &fids).Error; err != nil {
		t.Fatalf("чтение fid: %v", err)
	}
	return fids
}

// Граница чистки — позиция самого отставшего потребителя минус зазор.
func TestArchiver_Clean_KeepsGapBehindSlowestReader(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 200) // последовательная запись даёт блоб на сообщение

	setCounter(t, name, name+"_fast", 180, 180)
	setCounter(t, name, name+"_slow", 120, 120)
	setCounter(t, name, archiverCounterName(name), 150, 150)

	a := cleanArchiver(t, name, 20)
	if err := a.clean(); err != nil {
		t.Fatalf("clean: %v", err)
	}

	// самый отставший — 120, зазор 20, значит удалено всё до 100 включительно
	fids := remainingFIDs(t, name)
	if len(fids) == 0 {
		t.Fatal("удалено всё")
	}
	if fids[0] != 101 {
		t.Fatalf("первый оставшийся блоб fid=%d, ожидали 101", fids[0])
	}
}

// Архивер участвует в расчёте наравне с ридерами: если он отстал сильнее всех,
// граница считается по нему — это и не даёт чистке обогнать заливку в B2.
func TestArchiver_Clean_ArchiverIsAConsumerToo(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 200)

	setCounter(t, name, name+"_reader", 190, 190)
	setCounter(t, name, archiverCounterName(name), 60, 60) // архивер отстал сильнее всех

	a := cleanArchiver(t, name, 20)
	if err := a.clean(); err != nil {
		t.Fatalf("clean: %v", err)
	}

	fids := remainingFIDs(t, name)
	if fids[0] != 41 {
		t.Fatalf("первый оставшийся блоб fid=%d, ожидали 41 (60 − 20)", fids[0])
	}
}

// Зазор ещё не выбран — не удаляем ничего.
func TestArchiver_Clean_GapNotReached(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 50)

	setCounter(t, name, name+"_reader", 30, 30)

	a := cleanArchiver(t, name, 100)
	if err := a.clean(); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if got := len(remainingFIDs(t, name)); got != 50 {
		t.Fatalf("осталось %d блобов, ожидали все 50", got)
	}
}

// Очередь никто не читает — удалять не за кем, это защита от чистки вслепую.
func TestArchiver_Clean_NoConsumersNoDeletion(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 50)

	a := cleanArchiver(t, name, 10)
	if err := a.clean(); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if got := len(remainingFIDs(t, name)); got != 50 {
		t.Fatalf("осталось %d блобов, ожидали все 50", got)
	}
	if !a.CleanStats().NoReaders {
		t.Fatal("в статистике не отмечено отсутствие потребителей")
	}
}

// Удаление идёт батчами и доводится до конца, даже когда строк много больше
// одного батча.
func TestArchiver_Clean_DeletesInBatches(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 500)

	setCounter(t, name, name+"_reader", 450, 450)

	a := cleanArchiver(t, name, 50) // граница 400, батч 100 → четыре прохода
	if err := a.clean(); err != nil {
		t.Fatalf("clean: %v", err)
	}

	fids := remainingFIDs(t, name)
	if fids[0] != 401 {
		t.Fatalf("первый оставшийся блоб fid=%d, ожидали 401", fids[0])
	}
	if st := a.CleanStats(); st.DeletedRows != 400 {
		t.Fatalf("в статистике удалено %d строк, ожидали 400", st.DeletedRows)
	}
}

// Повторный проход без движения потребителей не должен ничего делать.
func TestArchiver_Clean_IdempotentWithoutProgress(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 200)
	setCounter(t, name, name+"_reader", 150, 150)

	a := cleanArchiver(t, name, 50)
	if err := a.clean(); err != nil {
		t.Fatalf("первый clean: %v", err)
	}
	first := a.CleanStats().DeletedRows

	if err := a.clean(); err != nil {
		t.Fatalf("второй clean: %v", err)
	}
	if got := a.CleanStats().DeletedRows; got != first {
		t.Fatalf("второй проход удалил ещё %d строк", got-first)
	}
}

// Метрики должны давать отставание самого медленного от головы очереди —
// по ним и вешается алерт, потому что сама чистка при затыке молчит.
func TestArchiver_Clean_StatsReportLag(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 300)
	setCounter(t, name, name+"_slow", 100, 100)

	a := cleanArchiver(t, name, 10)
	if err := a.clean(); err != nil {
		t.Fatalf("clean: %v", err)
	}

	st := a.CleanStats()
	if st.HeadFID != 300 {
		t.Fatalf("HeadFID=%d, ожидали 300", st.HeadFID)
	}
	if st.SlowestReader != name+"_slow" {
		t.Fatalf("самым медленным назван %q", st.SlowestReader)
	}
	if st.ReaderLag() != 200 {
		t.Fatalf("отставание %d, ожидали 200", st.ReaderLag())
	}
}
