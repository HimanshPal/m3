package block

import "github.com/m3db/m3/src/query/cost"

type AccountedBlock struct {
	Block

	enforcer *cost.ChainedEnforcer
}

func NewAccountedBlock(wrapped Block, enforcer *cost.ChainedEnforcer) *AccountedBlock {
	return &AccountedBlock{
		Block:    wrapped,
		enforcer: enforcer,
	}
}

func (ab *AccountedBlock) Close() error {
	ab.enforcer.Release()
	return ab.Block.Close()
}
