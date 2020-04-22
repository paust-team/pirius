package consumer

import (
	paustqproto "github.com/paust-team/paustq/proto"
)

type Condition interface {
	Check(interface{}) bool
}

type EndSubscriptionCondition struct {}
func NewEndSubscriptionCondition() *EndSubscriptionCondition {
	return &EndSubscriptionCondition{}
}

func (cond EndSubscriptionCondition) Eternal() *EndSubscriptionConditionEternal {
	return &EndSubscriptionConditionEternal{}
}

func (cond EndSubscriptionCondition) OnReachEnd() *EndSubscriptionConditionOnReachEnd {
	return &EndSubscriptionConditionOnReachEnd{}
}

type EndSubscriptionConditionEternal struct{}
func (cond EndSubscriptionConditionEternal) Check(_ interface{}) bool {
	return false
}

type EndSubscriptionConditionOnReachEnd struct{}
func (cond EndSubscriptionConditionOnReachEnd) Check(data interface{}) bool {
	fetchResponse := data.(*paustqproto.FetchResponse)

	if fetchResponse.LastOffset == fetchResponse.Offset {
		return true
	}
	return false
}