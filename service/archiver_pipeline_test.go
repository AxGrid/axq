package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
)

// fakeUploader подменяет B2: держит «залитые» файлы в памяти и умеет отказывать,
// чтобы проверить, что чистка не обгоняет незавершённую заливку.
type fakeUploader struct {
	mu    sync.Mutex
	files map[string][]byte
	order []string
	fail  error
}

func newFakeUploader() *fakeUploader {
	return &fakeUploader{files: map[string][]byte{}}
}

func (u *fakeUploader) Upload(filename string, _ map[string]string, data []byte) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.fail != nil {
		return u.fail
	}
	u.files[filename] = append([]byte(nil), data...)
	u.order = append(u.order, filename)
	return nil
}

func (u *fakeUploader) setFail(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.fail = err
}

func (u *fakeUploader) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.files)
}

func (u *fakeUploader) names() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.order...)
}

// messages разбирает залитый архив обратно — тот же путь, которым его будет
// читать b2-ридер.
func (u *fakeUploader) messages(t *testing.T, filename string) []uint64 {
	t.Helper()
	u.mu.Lock()
	raw, ok := u.files[filename]
	u.mu.Unlock()
	if !ok {
		t.Fatalf("файла %s нет в архиве", filename)
	}
	var blob protobuf.Blob
	if err := proto.Unmarshal(raw, &blob); err != nil {
		t.Fatalf("unmarshal архива: %v", err)
	}
	var list protobuf.BlobMessageList
	if err := proto.Unmarshal(blob.Messages, &list); err != nil {
		t.Fatalf("unmarshal списка сообщений: %v", err)
	}
	ids := make([]uint64, 0, len(list.Messages))
	for _, m := range list.Messages {
		ids = append(ids, m.Id)
	}
	return ids
}

func archiverOpts(t *testing.T, name string) domain.ArchiverOptions {
	return domain.ArchiverOptions{
		BaseOptions: domain.BaseOptions{
			Name:   name,
			Logger: zerolog.Nop(),
			CTX:    testCtx(t),
		},
		DB: domain.DataBaseOptions{DB: testDataBase},
		Reader: domain.ReaderOptions{
			LoaderCount: 2,
			WaiterCount: 1,
		},
		OuterCount: 1,
		ChunkSize:  100,
		MaxSize:    1 << 20,
		MaxCount:   50, // мелкие архивы, чтобы их набралось несколько
	}
}

func newFakeArchiver(t *testing.T, opts domain.ArchiverOptions, up blobUploader) *ArchiverService {
	t.Helper()
	a, err := newArchiverService(opts, up)
	if err != nil {
		t.Fatalf("newArchiverService: %v", err)
	}
	return a
}

// waitFor крутится до выполнения условия или падает по таймауту.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("не дождались: %s (за %s)", what, timeout)
}

// Сквозной конвейер: записанное доезжает до архива целиком и по порядку.
func TestArchiver_Pipeline_UploadsEverything(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	const n = 200
	pushN(t, w, n)

	up := newFakeUploader()
	a := newFakeArchiver(t, archiverOpts(t, name), up)

	waitFor(t, "архивация всей очереди", 60*time.Second, func() bool {
		return a.counters.Last().Id >= n
	})

	// собираем id из всех залитых файлов — должен получиться сплошной отрезок
	var all []uint64
	for _, fname := range up.names() {
		all = append(all, up.messages(t, fname)...)
	}
	assertContiguous(t, all, 1)
	assertAscending(t, all)
}

// Счётчик архивера обязан хранить табличный fid в fid и номер архива в b2_fid —
// на этом держится расчёт самого отставшего потребителя.
func TestArchiver_Pipeline_CounterKeepsTableFID(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	const n = 200
	pushN(t, w, n)

	up := newFakeUploader()
	a := newFakeArchiver(t, archiverOpts(t, name), up)

	waitFor(t, "архивация всей очереди", 60*time.Second, func() bool {
		return a.counters.Last().Id >= n
	})
	// счётчик сбрасывается в базу раз в 3 секунды
	time.Sleep(4 * time.Second)

	var row domain.BlobCounter
	err := testDataBase.Where("reader_name = ? AND name = ?", archiverCounterName(name), name).First(&row).Error
	if err != nil {
		t.Fatalf("чтение счётчика: %v", err)
	}

	// последовательная запись даёт блоб на сообщение, поэтому табличный fid
	// идёт вровень с id сообщения
	if row.Fid != row.ID {
		t.Fatalf("в fid счётчика %d при id=%d — это не позиция в таблице", row.Fid, row.ID)
	}
	if row.B2Fid == 0 {
		t.Fatal("b2_fid пустой: номер файла в архиве не сохранён")
	}
	if row.B2Fid >= row.Fid {
		t.Fatalf("b2_fid=%d не меньше табличного fid=%d, хотя в архив уходят пачки", row.B2Fid, row.Fid)
	}
}

// Нумерация архива обязана продолжаться после перезапуска, иначе новый архивер
// перезапишет уже залитые файлы.
func TestArchiver_Pipeline_B2NumberingSurvivesRestart(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	pushN(t, w, 200)

	up := newFakeUploader()
	first := newFakeArchiver(t, archiverOpts(t, name), up)
	waitFor(t, "первая порция архивов", 60*time.Second, func() bool {
		return first.counters.Last().Id >= 200
	})
	time.Sleep(4 * time.Second) // дать счётчику сохраниться
	first.Close()
	uploadedBefore := up.count()

	pushN2(t, w, 201, 200)

	second := newFakeArchiver(t, archiverOpts(t, name), up)
	waitFor(t, "вторая порция архивов", 60*time.Second, func() bool {
		return second.counters.Last().Id >= 400
	})

	if up.count() <= uploadedBefore {
		t.Fatalf("после перезапуска залито %d файлов против %d — новых нет", up.count(), uploadedBefore)
	}
	// имена файлов уникальны: ни один старый архив не перезаписан
	seen := map[string]bool{}
	for _, fname := range up.names() {
		if seen[fname] {
			t.Fatalf("файл %s залит повторно — архив перезаписан", fname)
		}
		seen[fname] = true
	}
}

// Чистка end-to-end: таблица подрезается, но зазор позади самого отставшего
// потребителя сохраняется.
func TestArchiver_Pipeline_CleanKeepsGap(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	const n = 400
	pushN(t, w, n)

	// сторонний потребитель, за которого чистка обязана держаться
	const slowFid = 300
	setCounter(t, name, name+"_slow", slowFid, slowFid)

	opts := archiverOpts(t, name)
	opts.CleanGapFID = 50
	opts.CleanInterval = 200 * time.Millisecond
	opts.CleanBatch = 100

	up := newFakeUploader()
	a := newFakeArchiver(t, opts, up)

	waitFor(t, "чистка отработала", 60*time.Second, func() bool {
		return a.CleanStats().DeletedRows > 0
	})
	waitFor(t, "чистка дошла до границы", 60*time.Second, func() bool {
		fids := remainingFIDs(t, name)
		return len(fids) > 0 && fids[0] == slowFid-opts.CleanGapFID+1
	})

	fids := remainingFIDs(t, name)
	if fids[0] != slowFid-opts.CleanGapFID+1 {
		t.Fatalf("первый оставшийся блоб fid=%d, ожидали %d", fids[0], slowFid-opts.CleanGapFID+1)
	}
	if st := a.CleanStats(); st.SlowestReader != name+"_slow" {
		t.Fatalf("самым медленным назван %q, ожидали %s_slow", st.SlowestReader, name)
	}
}

// Если заливка не проходит, счётчик архивера стоит — и чистка не имеет права
// удалять то, что до архива не доехало.
func TestArchiver_Pipeline_CleanWaitsForFailedUploads(t *testing.T) {
	name := testQueue(t)
	w := newWriter(t, writerOpts(t, name))
	const n = 400
	pushN(t, w, n)

	up := newFakeUploader()
	up.setFail(fmt.Errorf("b2 недоступен"))

	opts := archiverOpts(t, name)
	opts.CleanGapFID = 10
	opts.CleanInterval = 200 * time.Millisecond

	a := newFakeArchiver(t, opts, up)

	// дать чистке несколько проходов
	waitFor(t, "проход чистки", 30*time.Second, func() bool {
		return !a.CleanStats().LastRun.IsZero()
	})
	time.Sleep(time.Second)

	if got := len(remainingFIDs(t, name)); got != n {
		t.Fatalf("осталось %d блобов из %d — чистка обогнала неудавшуюся заливку", got, n)
	}
	if up.count() != 0 {
		t.Fatalf("залито %d файлов, хотя заливка отвергается", up.count())
	}
}
