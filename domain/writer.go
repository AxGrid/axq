/*
 * Created by Zed 08.12.2023, 14:43
 */

package domain

type Writer interface {
	Push(message []byte) error
	Close()
	GetOpts() ServiceOpts
	GetPerformance() uint64
}
