package sarama

import (
	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

func (h *Handler) handleStreamChannel(
	topic kafka.Topic,
	channel kafka.Handler,
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	forceCommit bool,
) (err error) {
	ctx := session.Context()
	for i := 0; i < len(h.before); i++ {
		ctx, err = h.before[i](ctx, session, claim)
		if err != nil {
			return
		}
	}
	message := &kafka.Message{
		Headers: make([]kafka.Header, 0, 8),
	}

	for {
		select {
		case msg := <-claim.Messages():
			h.convertMsg(message, msg)

			if err = channel.HandleMessage(ctx, message); err != nil {
				rebalanceOk := h.handleError(ctx, err)
				if rebalanceOk {
					return
				}
				err = nil
			}

			if forceCommit {
				session.Commit()
			} else {
				session.MarkMessage(msg, "")
			}

		case <-ctx.Done():
			return
		}
	}
}
