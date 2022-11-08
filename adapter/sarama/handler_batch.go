package sarama

import (
	"time"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

func (h *Handler) handleBatchChannel(
	channel kafka.BatchHandler,
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	forceCommit bool,
) (err error) {
	var index int
	var offset int64

	ctx := session.Context()
	for i := 0; i < len(h.before); i++ {
		ctx, err = h.before[i](ctx, session, claim)
		if err != nil {
			return
		}
	}

	buf := make([]*kafka.Message, h.batchSize)
	for i := 0; i < h.batchSize; i++ {
		buf[i] = &kafka.Message{
			Headers: make([]kafka.Header, 0, 8),
		}
	}

	timeout := time.NewTimer(h.batchWaitTimeout)
	defer func() {
		timeout.Stop()
	}()

	for {
		select {
		case msg := <-claim.Messages():
			h.convertMsg(buf[index], msg)
			index++
			timeout.Reset(h.batchWaitTimeout)
		next:
			for index < h.batchSize {
				select {
				case <-ctx.Done():
					return
				case msg = <-claim.Messages():
					h.convertMsg(buf[index], msg)
					index++
				case <-timeout.C:
					break next
				}
			}

			if err = channel.HandleMessages(ctx, buf, index); err != nil {
				needRebalance := h.handleError(ctx, err)
				if needRebalance {
					return
				}
				err = nil
			}

			if forceCommit {
				session.Commit()
			} else {
				if index > 0 {
					offset = buf[0].Offset
					for i := 0; i < index; i++ {
						offset++
						session.MarkOffset(claim.Topic(), claim.Partition(), offset, "")
					}
				}
			}
			index = 0
		case <-ctx.Done():
			return
		}
	}
}
