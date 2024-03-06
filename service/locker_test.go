/*
 * Created by Zed 06.12.2023, 10:16
 */

package service

import (
	"errors"
	"github.com/rs/zerolog/log"
	"gitlab.com/NebulousLabs/fastrand"
	"math/rand"
	"testing"
	"time"
)

func TestFidLock_Wait(t *testing.T) {
	wa := NewFidLock(1)

	go func() {
		_ = wa.WaitDo(3, func() error {
			log.Debug().Msg("3")
			return nil
		})

	}()

	go func() {
		_ = wa.WaitDo(2, func() error {
			log.Debug().Msg("2")
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)

}

func TestFidLock_Wait2(t *testing.T) {
	var el []int
	for i := 1; i < 100; i++ {
		el = append(el, i)
	}
	//shuffle
	rand.Shuffle(len(el), func(i, j int) {
		el[i], el[j] = el[j], el[i]
	})
	log.Info().Ints("el", el).Msg("shuffled")
	wa := NewFidLock(0)
	for _, v := range el {
		go func(v int) {
			for {
				err := wa.WaitDo(uint64(v), func() error {
					if fastrand.Intn(100) < 20 {
						log.Error().Int("v", v).Msg("error")
						return errors.New("test")
					}
					if fastrand.Intn(100) < 20 {
						time.Sleep(100 * time.Millisecond)
					}
					log.Debug().Int("v", v).Msg("got")
					return nil
				})
				if err == nil {
					break
				}
			}
		}(v)
	}

	time.Sleep(2000 * time.Millisecond)
}
