/*
 * Created by Zed 08.12.2023, 14:43
 */

package domain

type Writer interface {
	Push(message []byte) error
	Close()
	GetOpts() ServiceOpts
	Counter() (uint64, error)
	LastFID() (uint64, error)
	LastID() (uint64, error)
	Performance() uint64
}
