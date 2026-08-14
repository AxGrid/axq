package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

// W1: первая запись в пустую очередь задаёт начало отсчёта.
func TestWriter_FirstPush(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))

	if err := w.Push([]byte("hello")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	blobs := blobsOf(t, name)
	if len(blobs) != 1 {
		t.Fatalf("блобов %d, ожидали 1", len(blobs))
	}
	b := blobs[0]
	if b.FID != 1 || b.FromId != 1 || b.ToId != 1 || b.Total != 1 {
		t.Fatalf("блоб {FID:%d FromId:%d ToId:%d Total:%d}, ожидали {1 1 1 1}", b.FID, b.FromId, b.ToId, b.Total)
	}
}

// W2: нумерация сообщений сплошная, начиная с единицы.
func TestWriter_SequentialIDs(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	const n = 500
	pushN(t, w, n)

	lastId, err := w.LastID()
	if err != nil {
		t.Fatalf("LastID: %v", err)
	}
	if lastId != n {
		t.Fatalf("LastID=%d, ожидали %d", lastId, n)
	}
	assertBlobChainContiguous(t, name, n)
}

// W3: конкурентная запись не теряет и не дублирует id — save() раздаёт их
// в одиночку, и это единственная гарантия сплошной нумерации.
func TestWriter_ConcurrentPush(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))

	const n = 2000
	wg := sync.WaitGroup{}
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- w.Push([]byte(fmt.Sprintf("concurrent-%d", i)))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	lastId, _ := w.LastID()
	if lastId != n {
		t.Fatalf("LastID=%d, ожидали %d", lastId, n)
	}
	assertBlobChainContiguous(t, name, n)
}

// W4: PushMany сохраняет порядок переданных сообщений.
func TestWriter_PushManyKeepsOrder(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))

	msgs := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	if err := w.PushMany(msgs); err != nil {
		t.Fatalf("PushMany: %v", err)
	}

	r := newReader(t, readerOpts(t, name))
	got := make([]string, 0, len(msgs))
	deadline := time.After(10 * time.Second)
	for len(got) < len(msgs) {
		select {
		case m := <-r.C():
			got = append(got, string(m.Message()))
			m.Done()
		case <-deadline:
			t.Fatalf("прочитано %d из %d", len(got), len(msgs))
		}
	}
	for i, want := range []string{"a", "b", "c", "d"} {
		if got[i] != want {
			t.Fatalf("порядок нарушен: %v, ожидали [a b c d]", got)
		}
	}
}

// W5: блоб не может вырасти больше MaxBlobSize, иначе батч перестаёт быть
// предсказуемым по памяти.
func TestWriter_BlobNeverExceedsMaxBlobSize(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.MaxBlobSize = 50
	w := newWriter(t, opts)

	const n = 1000
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w.Push([]byte(fmt.Sprintf("m-%d", i)))
		}(i)
	}
	wg.Wait()

	for _, b := range blobsOf(t, name) {
		if b.Total > opts.MaxBlobSize {
			t.Fatalf("блоб fid=%d вместил %d сообщений при MaxBlobSize=%d", b.FID, b.Total, opts.MaxBlobSize)
		}
	}
	assertBlobChainContiguous(t, name, n)
}

// W6: краевое значение — каждое сообщение уезжает своим блобом.
func TestWriter_MaxBlobSizeOne(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.MaxBlobSize = 1
	w := newWriter(t, opts)

	const n = 20
	pushN(t, w, n)

	blobs := blobsOf(t, name)
	if len(blobs) != n {
		t.Fatalf("блобов %d, ожидали %d", len(blobs), n)
	}
	for _, b := range blobs {
		if b.Total != 1 {
			t.Fatalf("блоб fid=%d вместил %d сообщений при MaxBlobSize=1", b.FID, b.Total)
		}
	}
}

// W7: пустое и nil-сообщение — валидная полезная нагрузка, они должны
// сохраняться и возвращаться как есть.
func TestWriter_EmptyAndNilMessages(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))

	if err := w.Push([]byte{}); err != nil {
		t.Fatalf("Push пустого сообщения: %v", err)
	}
	if err := w.Push(nil); err != nil {
		t.Fatalf("Push nil: %v", err)
	}

	r := newReader(t, readerOpts(t, name))
	for i := 0; i < 2; i++ {
		select {
		case m := <-r.C():
			if len(m.Message()) != 0 {
				t.Fatalf("сообщение %d вернулось непустым: %q", i, m.Message())
			}
			m.Done()
		case <-time.After(10 * time.Second):
			t.Fatalf("не дождались сообщения %d", i)
		}
	}
}

// W7b: крупное сообщение проходит целиком, без обрезки.
func TestWriter_LargeMessage(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))

	large := make([]byte, 1<<20) // 1 МБ
	for i := range large {
		large[i] = byte(i % 251)
	}
	if err := w.Push(large); err != nil {
		t.Fatalf("Push 1МБ: %v", err)
	}

	r := newReader(t, readerOpts(t, name))
	select {
	case m := <-r.C():
		defer m.Done()
		if len(m.Message()) != len(large) {
			t.Fatalf("вернулось %d байт, отправляли %d", len(m.Message()), len(large))
		}
		for i := range large {
			if m.Message()[i] != large[i] {
				t.Fatalf("байт %d испорчен: %d вместо %d", i, m.Message()[i], large[i])
			}
		}
	case <-time.After(20 * time.Second):
		t.Fatal("не дождались крупного сообщения")
	}
}

// W9: после Close запись обязана отвергаться явной ошибкой.
func TestWriter_PushAfterClose(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 5)
	w.Close()

	if err := w.Push([]byte("late")); err == nil {
		t.Fatal("Push после Close вернул nil, ожидали ошибку")
	}
	if err := w.PushMany([][]byte{[]byte("late")}); err == nil {
		t.Fatal("PushMany после Close вернул nil, ожидали ошибку")
	}
}

// W10 — КРАСНЫЙ: Close обрывает запись на полпути.
//
// Close выставляет stopped, крутится до опустошения inChan и сразу дёргает
// cancelFunc. Но сообщения, уже вынутые из канала в blobList, к этому моменту
// ещё не записаны: save() стоит на `<-bCreate.res`, а create() по отменённому
// ctx выходит, не ответив. В итоге Push виснет навсегда, а данные теряются.
// Окно узкое — оно длится ровно один поход в базу, — поэтому Close зовётся
// посреди непрерывного потока записи, а не после короткой пачки.
func TestWriter_CloseFlushesInFlight(t *testing.T) {
	// Попасть в окно с первого раза удаётся не всегда, поэтому пробуем
	// несколько раз: одного воспроизведения достаточно, чтобы признать баг.
	for attempt := 1; attempt <= 5; attempt++ {
		closeUnderLoad(t, attempt)
	}
}

func closeUnderLoad(t *testing.T, attempt int) {
	t.Helper()
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.MaxBlobSize = 500
	w := newWriter(t, opts)

	var (
		mu    sync.Mutex
		acked uint64
	)
	wg := sync.WaitGroup{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; ; n++ {
				if err := w.Push([]byte(fmt.Sprintf("inflight-%d-%d", i, n))); err != nil {
					return // writer stopped
				}
				mu.Lock()
				acked++
				mu.Unlock()
			}
		}(i)
	}
	time.Sleep(300 * time.Millisecond) // дать потоку раскрутиться
	w.Close()

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatalf("попытка %d: после Close часть Push зависла навсегда — батч, вынутый из inChan, некому дозаписать", attempt)
	}

	mu.Lock()
	confirmed := acked
	mu.Unlock()

	lastId, err := w.LastID()
	if err != nil {
		t.Fatalf("LastID: %v", err)
	}
	if lastId < confirmed {
		t.Fatalf("попытка %d: Push подтвердил %d сообщений, а осело только %d — Close потерял данные", attempt, confirmed, lastId)
	}
}

// W11: новый сервис на непустой таблице продолжает нумерацию, а не начинает
// заново — иначе перезапуск затирал бы историю.
func TestWriter_RestartContinuesNumbering(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)

	first := newWriter(t, opts)
	pushN(t, first, 100)

	second := newWriter(t, opts)
	if err := second.Push([]byte("after restart")); err != nil {
		t.Fatalf("Push после перезапуска: %v", err)
	}

	lastId, _ := second.LastID()
	if lastId != 101 {
		t.Fatalf("после перезапуска LastID=%d, ожидали 101", lastId)
	}
	assertBlobChainContiguous(t, name, 101)
}

// W12: очередь принадлежит одному владельцу — чужой UUID должен получить отказ,
// иначе два райтера начнут раздавать одни и те же id.
func TestWriter_ForeignUUIDRejected(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.UUID = uuid.New()
	newWriter(t, opts)

	other := writerOpts(t, name)
	other.UUID = uuid.New()
	if _, err := NewWriterService(other); err == nil {
		t.Fatal("райтер с чужим UUID создался, ожидали отказ")
	}
}

// W13: тот же владелец подключается повторно без возражений.
func TestWriter_SameUUIDAccepted(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.UUID = uuid.New()

	newWriter(t, opts)
	if _, err := NewWriterService(opts); err != nil {
		t.Fatalf("повторное подключение того же владельца: %v", err)
	}
}

// W14: режим сжатия и шифрования записывается в сам блоб — по нему ридер
// потом решает, как его разбирать.
func TestWriter_CompressionFlagsStored(t *testing.T) {
	for _, tc := range []struct {
		title       string
		compression domain.BlobCompression
		encryption  domain.BlobEncryption
	}{
		{"none", domain.BLOB_COMPRESSION_NONE, domain.BLOB_ENCRYPTION_NONE},
		{"gzip", domain.BLOB_COMPRESSION_GZIP, domain.BLOB_ENCRYPTION_NONE},
		{"aes", domain.BLOB_COMPRESSION_NONE, domain.BLOB_ENCRYPTION_AES},
		{"gzip+aes", domain.BLOB_COMPRESSION_GZIP, domain.BLOB_ENCRYPTION_AES},
	} {
		t.Run(tc.title, func(t *testing.T) {
			name := testQueue(t)
			opts := writerOpts(t, name)
			opts.DB.Compression = domain.CompressionOptions{
				Compression:   tc.compression,
				Encryption:    tc.encryption,
				EncryptionKey: []byte("12345678901234567890123456789012"),
			}
			w := newWriter(t, opts)
			pushN(t, w, 10)

			blobs := blobsOf(t, name)
			if len(blobs) == 0 {
				t.Fatal("блобов нет")
			}
			if blobs[0].Compression != tc.compression {
				t.Fatalf("Compression=%v, ожидали %v", blobs[0].Compression, tc.compression)
			}
			if blobs[0].Encryption != tc.encryption {
				t.Fatalf("Encryption=%v, ожидали %v", blobs[0].Encryption, tc.encryption)
			}
		})
	}
}

// W15: ключ шифрования обязан быть ровно 32 байта, иначе AES не построить.
func TestWriter_BadEncryptionKeyRejected(t *testing.T) {
	for _, key := range [][]byte{nil, []byte("short"), []byte("123456789012345678901234567890123")} {
		name := testQueue(t)
		opts := writerOpts(t, name)
		opts.DB.Compression = domain.CompressionOptions{
			Encryption:    domain.BLOB_ENCRYPTION_AES,
			EncryptionKey: key,
		}
		if _, err := NewWriterService(opts); err == nil {
			t.Fatalf("ключ длиной %d принят, ожидали отказ", len(key))
		}
	}
}

// W16: PartitionsCount>1 создаёт партиционированную таблицу, <=1 — обычную.
// Ветка без партиций существует ради движков, которые их не понимают.
func TestWriter_Partitioning(t *testing.T) {
	for _, tc := range []struct {
		title          string
		count          int
		wantPartitions bool
	}{
		{"без партиций", 1, false},
		{"четыре партиции", 4, true},
	} {
		t.Run(tc.title, func(t *testing.T) {
			name := testQueue(t)
			opts := writerOpts(t, name)
			opts.PartitionsCount = tc.count
			w := newWriter(t, opts)
			pushN(t, w, 10)

			var partitions int64
			err := testDataBase.Raw(
				`SELECT COUNT(*) FROM information_schema.partitions
				 WHERE table_schema = DATABASE() AND table_name = ? AND partition_name IS NOT NULL`,
				"axq_"+name,
			).Scan(&partitions).Error
			if err != nil {
				t.Fatalf("information_schema: %v", err)
			}
			if tc.wantPartitions && partitions == 0 {
				t.Fatalf("PartitionsCount=%d, а таблица не партиционирована", tc.count)
			}
			if !tc.wantPartitions && partitions != 0 {
				t.Fatalf("PartitionsCount=%d, а у таблицы %d партиций", tc.count, partitions)
			}
		})
	}
}

// W17: protobuf-обёртки над Push должны давать тот же результат, что и байты.
func TestWriter_PushProto(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))

	one := &protobuf.BlobMessage{Id: 42, Message: []byte("proto-one")}
	if err := w.PushProto(one); err != nil {
		t.Fatalf("PushProto: %v", err)
	}
	many := []proto.Message{
		&protobuf.BlobMessage{Id: 43, Message: []byte("proto-two")},
		&protobuf.BlobMessage{Id: 44, Message: []byte("proto-three")},
	}
	if err := w.PushProtoMany(many); err != nil {
		t.Fatalf("PushProtoMany: %v", err)
	}

	r := newReader(t, readerOpts(t, name))
	wantIds := []uint64{42, 43, 44}
	for i, wantId := range wantIds {
		select {
		case m := <-r.C():
			var got protobuf.BlobMessage
			if err := m.UnmarshalProto(&got); err != nil {
				t.Fatalf("UnmarshalProto #%d: %v", i, err)
			}
			if got.Id != wantId {
				t.Fatalf("сообщение %d содержит Id=%d, ожидали %d", i, got.Id, wantId)
			}
			m.Done()
		case <-time.After(10 * time.Second):
			t.Fatalf("не дождались сообщения %d", i)
		}
	}
}

// W18: если запись в базу перестала проходить, ожидающие Push обязаны получить
// ошибку, а не зависнуть навсегда.
func TestWriter_DBErrorPropagatesToPush(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 1)

	if err := testDataBase.Exec("DROP TABLE axq_" + name).Error; err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Push([]byte("after drop")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Push в удалённую таблицу вернул nil, ожидали ошибку")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Push завис вместо возврата ошибки")
	}
}

// W19: на пустой очереди все позиции нулевые, а не ошибка.
func TestWriter_PositionsOnEmptyQueue(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))

	for _, tc := range []struct {
		title string
		fn    func() (uint64, error)
	}{
		{"LastFID", w.LastFID},
		{"LastID", w.LastID},
		{"MinimalFID", w.MinimalFID},
		{"MinimalID", w.MinimalID},
	} {
		got, err := tc.fn()
		if err != nil {
			t.Fatalf("%s на пустой очереди: %v", tc.title, err)
		}
		if got != 0 {
			t.Fatalf("%s на пустой очереди вернул %d, ожидали 0", tc.title, got)
		}
	}
}

// W19b: позиции на заполненной очереди указывают на реальные границы.
func TestWriter_PositionsWithData(t *testing.T) {
	name := testQueue(t)
	opts := writerOpts(t, name)
	opts.MaxBlobSize = 10
	w := newWriter(t, opts)
	pushN(t, w, 100)

	blobs := blobsOf(t, name)
	first, last := blobs[0], blobs[len(blobs)-1]

	check := func(title string, fn func() (uint64, error), want uint64) {
		got, err := fn()
		if err != nil {
			t.Fatalf("%s: %v", title, err)
		}
		if got != want {
			t.Fatalf("%s=%d, ожидали %d", title, got, want)
		}
	}
	check("LastFID", w.LastFID, last.FID)
	check("LastID", w.LastID, last.ToId)
	check("MinimalFID", w.MinimalFID, first.FID)
	check("MinimalID", w.MinimalID, first.FromId)
}

// assertBlobChainContiguous проверяет, что блобы очереди покрывают отрезок
// [1, total] встык: fid растут по единице, диапазоны сообщений не рвутся и не
// перекрываются.
func assertBlobChainContiguous(t *testing.T, name string, total int) {
	t.Helper()
	// payload тут не нужен, а на нагрузочных прогонах это десятки мегабайт
	var blobs []domain.Blob
	if err := testDataBase.Table("axq_"+name).
		Select("fid", "from_id", "to_id", "total").
		Order("fid asc").Find(&blobs).Error; err != nil {
		t.Fatalf("чтение блобов: %v", err)
	}
	if len(blobs) == 0 {
		t.Fatal("блобов нет")
	}
	var (
		wantFID uint64 = 1
		wantId  uint64 = 1
		seen    int
	)
	for _, b := range blobs {
		if b.FID != wantFID {
			t.Fatalf("разрыв в fid: получили %d, ожидали %d", b.FID, wantFID)
		}
		if b.FromId != wantId {
			t.Fatalf("блоб fid=%d начинается с id=%d, ожидали %d", b.FID, b.FromId, wantId)
		}
		if b.ToId < b.FromId {
			t.Fatalf("блоб fid=%d имеет ToId=%d < FromId=%d", b.FID, b.ToId, b.FromId)
		}
		if got := int(b.ToId - b.FromId + 1); got != b.Total {
			t.Fatalf("блоб fid=%d: диапазон покрывает %d сообщений, а Total=%d", b.FID, got, b.Total)
		}
		seen += b.Total
		wantFID++
		wantId = b.ToId + 1
	}
	if seen != total {
		t.Fatalf("в блобах %d сообщений, записывали %d", seen, total)
	}
}
