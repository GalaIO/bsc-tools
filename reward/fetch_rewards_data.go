package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/GalaIO/bsc-tools/utils"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	logger "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

var (
	DustyIncoming       = big.NewInt(1e17)
	Precision           = big.NewInt(1e10)
	SharingPercent      = big.NewInt(50)
	SharingTotalPercent = big.NewInt(100)
	DelayInEmptyFetch   = 100 * time.Millisecond
	DelayInParseEvent   = 100 * time.Millisecond
	DelayInLogFilter    = 1000 * time.Millisecond
)

var (
	ErrNotFoundLog  = errors.New("not found log")
	ErrNotFoundTerm = errors.New("not found term")
)

type DepositItem struct {
	Address string
	Deposit *big.Int
	Sharing *big.Int
}

type SlashItem struct {
	Address  string
	Amount   *big.Int
	IsFelony bool
}

func NewDepositItem(address string, deposit *big.Int, sharing *big.Int) *DepositItem {
	return &DepositItem{
		Address: address,
		Deposit: deposit,
		Sharing: sharing,
	}
}

func NewSlashItem(address string, amount *big.Int, isFelony bool) *SlashItem {
	return &SlashItem{
		Address:  address,
		Amount:   amount,
		IsFelony: isFelony,
	}
}

type Validator struct {
	Address       string
	FeeAddress    string
	BBCFeeAddress string
	VotingPower   uint64

	Jailed        bool     `rlp:"-"`
	Incoming      *big.Int `rlp:"-"`
	IsMaintaining bool     `rlp:"-"`
	BlockCounter  uint64   `rlp:"-"`
}

func DefaultValidator(address, feeAddress, BBCFeeAddress string) *Validator {
	return &Validator{
		Address:       address,
		FeeAddress:    feeAddress,
		BBCFeeAddress: BBCFeeAddress,
		Jailed:        false,
		Incoming:      new(big.Int),
		IsMaintaining: false,
		BlockCounter:  0,
	}
}

// calculateReward
// ignore jailed slash
func calculateReward10(validators map[string]*Validator, deposits map[uint64]*DepositItem, slashes map[uint64]*SlashItem) *RewardInfo {
	totalInComing := big.NewInt(0)
	balance := big.NewInt(0)

	blks := make([]uint64, 0, len(deposits))
	for i := range deposits {
		blks = append(blks, i)
	}
	sort.Sort(UInt64Slice(blks))

	for _, i := range blks {
		item := deposits[i]
		balance.Add(balance, item.Deposit)
		balance.Add(balance, item.Sharing)
		val, ok := validators[item.Address]
		// not found or jailed
		if !ok || val.Jailed {
			continue
		}

		// slash first
		if slashItem, existSlash := slashes[i]; existSlash {
			slashValidator(validators, slashItem)
			if slashItem.IsFelony {
				delete(validators, slashItem.Address)
			}
		}

		totalInComing.Add(totalInComing, item.Deposit)
		totalInComing.Add(totalInComing, item.Sharing)

		val.Incoming.Add(val.Incoming, item.Deposit)
		val.Incoming.Add(val.Incoming, item.Sharing)
		logger.Debug("reward %v, total %v", val.Address, val.Incoming)
	}

	crossTransfer := big.NewInt(0)
	directTransfer := big.NewInt(0)
	// read from contract
	relayFee := big.NewInt(2e15)
	rewards := make(map[string]*big.Int, len(validators))
	// cross or direct
	for _, item := range validators {
		if item.Incoming.Cmp(DustyIncoming) >= 0 {
			// expect little
			item.Incoming.Sub(item.Incoming, new(big.Int).Mod(item.Incoming, Precision))
			crossTransfer.Add(crossTransfer, item.Incoming)
			item.Incoming.Sub(item.Incoming, relayFee)
			rewards[item.Address] = item.Incoming
			logger.Debug("reward %v:%v crossTransfer %v", item.Address, item.BBCFeeAddress, item.Incoming)
			continue
		}
		rewards[item.Address] = item.Incoming
		directTransfer.Add(directTransfer, item.Incoming)
		logger.Debug("reward %v:%v directTransfer %v", item.Address, item.BBCFeeAddress, item.Incoming)
	}
	logger.Debug("transfer to crossTransfer %v, directTransfer %v", crossTransfer, directTransfer)

	// left
	balance.Sub(balance, crossTransfer)
	balance.Sub(balance, directTransfer)
	logger.Debug("transfer to systemReward %v\n-------------", balance)
	return NewRewardInfo(rewards, balance)
}

func slashValidator(validators map[string]*Validator, slashItem *SlashItem) {
	slashVal, exist := validators[slashItem.Address]
	if !exist {
		return
	}
	avg := slashVal.Incoming
	slashVal.Incoming = new(big.Int).SetUint64(0)
	count := len(validators) - 1
	if count <= 0 {
		return
	}

	avg.Div(avg, big.NewInt(int64(count)))
	for k, v := range validators {
		if k == slashItem.Address {
			continue
		}
		v.Incoming.Add(v.Incoming, avg)
	}
}

// calculateReward
// ignore jailed slash
func calculateReward20(validators map[string]*Validator, deposits map[uint64]*DepositItem, slashes map[uint64]*SlashItem, enable bool) *RewardInfo {
	totalInComing := big.NewInt(0)
	sharingRewardFundingPool := big.NewInt(0)
	balance := big.NewInt(0)

	blks := make([]uint64, 0, len(deposits))
	for i := range deposits {
		blks = append(blks, i)
	}
	sort.Sort(UInt64Slice(blks))

	for _, i := range blks {
		item := deposits[i]
		val, ok := validators[item.Address]
		balance.Add(balance, item.Deposit)
		balance.Add(balance, item.Sharing)
		// not found or jailed
		if !ok || val.Jailed {
			continue
		}

		// slash first
		if slashItem, existSlash := slashes[i]; existSlash {
			slashValidator(validators, slashItem)
			slashVal, exist := validators[slashItem.Address]
			if exist {
				slashVal.BlockCounter = 0
			}
			if slashItem.IsFelony {
				delete(validators, slashItem.Address)
			}
		}

		totalInComing.Add(totalInComing, item.Deposit)
		totalInComing.Add(totalInComing, item.Sharing)

		if enable {
			val.Incoming.Add(val.Incoming, item.Deposit)
			sharingRewardFundingPool.Add(sharingRewardFundingPool, item.Sharing)
			logger.Debug("reward %v, dedicated %v, sharing %v", val.Address, item.Deposit, item.Sharing)
		} else {
			sharing := new(big.Int).Mul(item.Deposit, SharingPercent)
			sharing.Div(sharing, SharingTotalPercent)

			dedicated := new(big.Int).Sub(item.Deposit, sharing)
			val.Incoming.Add(val.Incoming, dedicated)
			sharingRewardFundingPool.Add(sharingRewardFundingPool, sharing)
			logger.Debug("reward %v, dedicated %v, sharing %v", val.Address, dedicated, sharing)
		}
		val.BlockCounter += 1
	}

	// distribute
	totalBlock := uint64(0)
	for _, item := range validators {
		totalBlock += item.BlockCounter
	}
	if totalBlock > 0 {
		for _, item := range validators {
			addition := new(big.Int).Mul(sharingRewardFundingPool, new(big.Int).SetUint64(item.BlockCounter))
			addition.Div(addition, new(big.Int).SetUint64(totalBlock))
			item.Incoming.Add(item.Incoming, addition)
		}
	}

	crossTransfer := big.NewInt(0)
	directTransfer := big.NewInt(0)
	// read from contract
	relayFee := big.NewInt(2e15)
	rewards := make(map[string]*big.Int, len(validators))
	// cross or direct
	for _, item := range validators {
		if item.Incoming.Cmp(DustyIncoming) >= 0 {
			// expect little
			item.Incoming.Sub(item.Incoming, new(big.Int).Mod(item.Incoming, Precision))
			crossTransfer.Add(crossTransfer, item.Incoming)
			item.Incoming.Sub(item.Incoming, relayFee)
			rewards[item.Address] = item.Incoming
			logger.Debug("reward %v:%v crossTransfer %v", item.Address, item.BBCFeeAddress, item.Incoming)
			continue
		}
		rewards[item.Address] = item.Incoming
		directTransfer.Add(directTransfer, item.Incoming)
		logger.Debug("reward %v:%v directTransfer %v", item.Address, item.BBCFeeAddress, item.Incoming)
	}
	logger.Debug("transfer to crossTransfer %v, directTransfer %v", crossTransfer, directTransfer)

	// left
	balance.Sub(balance, crossTransfer)
	balance.Sub(balance, directTransfer)
	logger.Debug("transfer to systemReward %v\n-------------", balance)
	return NewRewardInfo(rewards, balance)
}

type ABIWrapper struct {
	name string
	path string
	abi  *abi.ABI
}

func LoadFromAbiDir(name string) *ABIWrapper {
	dir, err := os.Getwd()
	utils.PanicErr(err)
	path := filepath.Join(dir, "reward", "abi", fmt.Sprintf("%s.abi.json", name))
	abiFile, err := os.Open(path)
	utils.PanicErr(err)
	a, err := abi.JSON(abiFile)
	utils.PanicErr(err)
	return &ABIWrapper{
		name: name,
		path: path,
		abi:  &a,
	}
}

type EventWrapper struct {
	contractName string
	ev           abi.Event
}

type FuncWrapper struct {
	contractName string
	me           abi.Method
}

func (f *FuncWrapper) ParseArgs(data []byte) ([]interface{}, error) {
	return f.me.Inputs.UnpackValues(data[4:])
}

type RLPValidator struct {
	Address       common.Address
	FeeAddress    common.Address
	BBCFeeAddress common.Address
	VotingPower   uint64
}

type IbcValidatorSetPackage struct {
	PackageType  uint8
	ValidatorSet []*RLPValidator
}

func LoadValidatorSetPkgFromRLPBytes(data []byte) *IbcValidatorSetPackage {
	val := new(IbcValidatorSetPackage)
	err := rlp.DecodeBytes(data, val)
	utils.PanicErr(err)
	return val
}

type TransferOutSynPackage struct {
	Bep2TokenSymbol big.Int
	ContractAddr    common.Address
	Amounts         []big.Int
	Recipients      []common.Address
	RefundAddrs     []common.Address
	ExpireTime      big.Int
}

func LoadTransferOutPkgFromRLPBytes(data []byte) *TransferOutSynPackage {
	val := new(TransferOutSynPackage)
	err := rlp.DecodeBytes(data, val)
	utils.PanicErr(err)
	return val
}

// ValidatorTerm store validator reward info, after every breath block/validatorSetUpdated
type ValidatorTerm struct {
	StartBlock          uint64
	StartBlockTimestamp uint64
	EndBlock            uint64
	EndBlockTimestamp   uint64
	TotalIncoming       *big.Int
	SharingFundingPool  *big.Int
	Validators          map[string]*Validator
	DepositMapByBlk     map[uint64]*DepositItem //record every block deposit
	SlashMapByBlk       map[uint64]*SlashItem   //record every block slash
	RewardSummary       *RewardInfo
}

func NewValidatorTerm(block *types.Block, validators []*Validator) *ValidatorTerm {
	valMap := make(map[string]*Validator, len(validators))
	for _, val := range validators {
		valMap[val.Address] = val
	}
	return &ValidatorTerm{
		StartBlock:          block.NumberU64(),
		StartBlockTimestamp: block.Time(),
		EndBlock:            0,
		EndBlockTimestamp:   0,
		TotalIncoming:       new(big.Int).SetUint64(0),
		SharingFundingPool:  new(big.Int).SetUint64(0),
		Validators:          valMap,
		DepositMapByBlk:     make(map[uint64]*DepositItem, 64),
		SlashMapByBlk:       make(map[uint64]*SlashItem, 64),
	}
}

type RewardInfo struct {
	ValidatorReward map[string]*big.Int
	SystemReward    *big.Int
}

func (r *RewardInfo) String() string {
	var s string
	s += fmt.Sprintf("==============================\n")
	vw := make([]string, 0, len(r.ValidatorReward))
	for k, _ := range r.ValidatorReward {
		vw = append(vw, k)
	}
	sort.Strings(vw)
	for _, v := range vw {
		s += fmt.Sprintf("[Validator] %v %v\n", v, r.ValidatorReward[v])
	}
	s += fmt.Sprintf("[System reward] %v\n", r.SystemReward.String())
	s += fmt.Sprintf("==============================\n")
	return s
}

func NewRewardInfo(vals map[string]*big.Int, sysReward *big.Int) *RewardInfo {
	return &RewardInfo{
		ValidatorReward: vals,
		SystemReward:    sysReward,
	}
}

type RewardContext struct {
	Terms          []*ValidatorTerm
	EnableReward20 bool
}

func NewRewardContext(enableReward20 bool) *RewardContext {
	return &RewardContext{
		Terms:          make([]*ValidatorTerm, 0, 64),
		EnableReward20: enableReward20,
	}
}

func (r *RewardContext) NewTerm(term *ValidatorTerm) error {
	termLen := len(r.Terms)
	if termLen > 0 {
		cur := r.Terms[termLen-1]
		// TODO check?
		if cur.EndBlock > term.StartBlock {
			return errors.New("NewTerm wrong term")
		}
	}
	r.Terms = append(r.Terms, term)
	return nil
}

func (r *RewardContext) CurTerm() (*ValidatorTerm, error) {
	termLen := len(r.Terms)
	if termLen == 0 {
		return nil, ErrNotFoundTerm
	}
	cur := r.Terms[termLen-1]

	return cur, nil
}

func (r *RewardContext) TerminalTerm(client *ethclient.Client, block *types.Block, rewardInfo *RewardInfo) error {
	termLen := len(r.Terms)
	if termLen == 0 {
		return errors.New("NewTerm no term to terminal")
	}
	cur := r.Terms[termLen-1]
	cur.EndBlock = block.NumberU64()
	cur.EndBlockTimestamp = block.Time()
	cur.RewardSummary = rewardInfo

	// calculate by myself
	logger.Info("[Update validator set] from %v to %v from %v to %v",
		cur.StartBlock, cur.EndBlock, cur.StartBlockTimestamp, cur.EndBlockTimestamp)
	logger.Info("[Reward parsed]\n%v", rewardInfo)

	deposits := cur.DepositMapByBlk
	slashes := cur.SlashMapByBlk
	validators := cur.Validators
	newVals := make(map[string]*Validator, len(validators))
	for k, v := range validators {
		newVals[k] = &Validator{
			Address:       v.Address,
			FeeAddress:    v.FeeAddress,
			BBCFeeAddress: v.FeeAddress,
			VotingPower:   v.VotingPower,
			Jailed:        v.Jailed,
			IsMaintaining: v.IsMaintaining,
			Incoming:      big.NewInt(0),
			BlockCounter:  0,
		}
	}
	logger.Info("[Reward 1.0 review]\n%v", calculateReward10(newVals, deposits, slashes))

	newVals = make(map[string]*Validator, len(validators))
	for k, v := range validators {
		newVals[k] = &Validator{
			Address:       v.Address,
			FeeAddress:    v.FeeAddress,
			BBCFeeAddress: v.FeeAddress,
			VotingPower:   v.VotingPower,
			Jailed:        v.Jailed,
			IsMaintaining: v.IsMaintaining,
			Incoming:      big.NewInt(0),
			BlockCounter:  0,
		}
	}
	// handle empty reward block especially
	for i := cur.StartBlock; i < cur.EndBlock; i++ {
		_, exist := deposits[i]
		if exist {
			continue
		}

		blk, err := client.BlockByNumber(context.Background(), new(big.Int).SetUint64(i))
		if err != nil {
			return err
		}
		logger.Info("[Find empty block] %v %v", blk.Number().Uint64(), blk.Coinbase())
		coinbase := blk.Coinbase().String()
		val, ok := newVals[coinbase]
		// not found or jailed
		if !ok || val.Jailed {
			continue
		}
		deposits[i] = NewDepositItem(coinbase, big.NewInt(0), big.NewInt(0))
		time.Sleep(DelayInEmptyFetch)
	}
	logger.Info("[Reward 2.0 review]\n%v", calculateReward20(newVals, deposits, slashes, r.EnableReward20))

	return nil
}

func (r *RewardContext) RecordDeposit(block uint64, address string, deposit, sharing *big.Int) error {
	termLen := len(r.Terms)
	if termLen < 1 {
		return errors.New("RecordDeposit empty terms")
	}
	cur := r.Terms[termLen-1]
	if _, ok := cur.DepositMapByBlk[block]; ok {
		return errors.New("RecordDeposit duplicated block info")
	}
	if cur.StartBlock > block {
		return errors.New("RecordDeposit err to this term")
	}
	if cur.EndBlock > 0 {
		return errors.New("RecordDeposit err term done")
	}

	// record every block
	cur.DepositMapByBlk[block] = NewDepositItem(address, deposit, sharing)

	// record reward
	cur.SharingFundingPool.Add(cur.SharingFundingPool, sharing)
	cur.TotalIncoming.Add(cur.TotalIncoming, deposit)
	cur.TotalIncoming.Add(cur.TotalIncoming, sharing)

	// record block
	cur.Validators[address].BlockCounter += 1
	return nil
}

func (r *RewardContext) RecordSlash(block uint64, address string, amount *big.Int, isFelony bool) error {
	termLen := len(r.Terms)
	if termLen < 1 {
		return errors.New("RecordDeposit empty terms")
	}
	cur := r.Terms[termLen-1]
	if _, ok := cur.DepositMapByBlk[block]; ok {
		return errors.New("RecordDeposit duplicated block info")
	}
	if cur.StartBlock > block {
		return errors.New("RecordDeposit err to this term")
	}
	if cur.EndBlock > 0 {
		return errors.New("RecordDeposit err term done")
	}

	// record every block
	cur.SlashMapByBlk[block] = NewSlashItem(address, amount, isFelony)
	return nil
}

func NewEventWrapper(aw *ABIWrapper, name string) *EventWrapper {
	ev, ok := aw.abi.Events[name]
	if !ok {
		panic(fmt.Sprintf("not found %v from %v", name, aw.name))
	}
	return &EventWrapper{
		contractName: aw.name,
		ev:           ev,
	}
}

func NewFuncWrapper(aw *ABIWrapper, name string) *FuncWrapper {
	me, ok := aw.abi.Methods[name]
	if !ok {
		panic(fmt.Sprintf("not found %v from %v", name, aw.name))
	}
	return &FuncWrapper{
		contractName: aw.name,
		me:           me,
	}
}

func (e *EventWrapper) FilterAndParseLog(logs []*types.Log) ([][]interface{}, error) {
	ret := make([][]interface{}, 0, len(logs))
	for _, log := range logs {
		if e.ev.ID == log.Topics[0] {
			vals, err := e.ParseLog(log)
			if err != nil {
				return nil, err
			}
			ret = append(ret, vals)
		}
	}
	if len(ret) == 0 {
		return nil, ErrNotFoundLog
	}
	return ret, nil
}

func (e *EventWrapper) ParseLog(log *types.Log) ([]interface{}, error) {
	unindexed := make(map[string]interface{})
	if err := e.ev.Inputs.UnpackIntoMap(unindexed, log.Data); err != nil {
		return nil, err
	}

	indexed := make(map[string]interface{})
	indexedArgs := make([]abi.Argument, 0, 10)
	for _, a := range e.ev.Inputs {
		if a.Indexed {
			indexedArgs = append(indexedArgs, a)
		}
	}
	if err := abi.ParseTopicsIntoMap(indexed, indexedArgs, log.Topics[1:]); err != nil {
		return nil, err
	}

	// reorder
	vals := make([]interface{}, 0, len(e.ev.Inputs))
	for _, a := range e.ev.Inputs {
		if a.Indexed {
			val, ok := indexed[a.Name]
			if !ok {
				return nil, errors.New(fmt.Sprintf("cannot find %v from indexed", a.Name))
			}
			vals = append(vals, val)
			continue
		}
		val, ok := unindexed[a.Name]
		if !ok {
			return nil, errors.New(fmt.Sprintf("cannot find %v from unindexed", a.Name))
		}
		vals = append(vals, val)
	}

	return vals, nil
}

type EventHandler struct {
	event   *EventWrapper
	handler func(log []*types.Log) error
}

func NewEventHandler(event *EventWrapper, handler func(log []*types.Log) error) *EventHandler {
	return &EventHandler{
		event:   event,
		handler: handler,
	}
}

var (
	// load abi
	validatorSetAbi = LoadFromAbiDir("BSCValidatorSet")
	crossChainAbi   = LoadFromAbiDir("CrossChain")

	// eventList
	//validatorSet events
	//event validatorDeposit(Address indexed validator, uint256 amount);
	validatorDepositEvent = NewEventWrapper(validatorSetAbi, "validatorDeposit")
	//event validatorSharing(Address indexed validator, uint256 amount);
	validatorSharingEvent = NewEventWrapper(validatorSetAbi, "validatorSharing")
	//event validatorMisdemeanor(Address indexed validator, uint256 amount);
	validatorMisdemeanorEvent = NewEventWrapper(validatorSetAbi, "validatorMisdemeanor")
	//event validatorFelony(Address indexed validator, uint256 amount);
	validatorFelonyEvent = NewEventWrapper(validatorSetAbi, "validatorFelony")
	//event validatorSetUpdated();
	validatorSetUpdatedEvent = NewEventWrapper(validatorSetAbi, "validatorSetUpdated")
	//event directTransfer(Address payable indexed validator, uint256 amount);
	directTransferEvent = NewEventWrapper(validatorSetAbi, "directTransfer")
	//event systemTransfer(uint256 amount);
	systemTransferEvent = NewEventWrapper(validatorSetAbi, "systemTransfer")

	//crossChain
	//event crossChainPackage(uint16 chainId, uint64 indexed oracleSequence, uint64 indexed packageSequence, uint8 indexed channelId, bytes payload);
	crossChainPackageEvent = NewEventWrapper(crossChainAbi, "crossChainPackage")

	// funcList
	//function handlePackage(bytes calldata payload, bytes calldata proof, uint64 height, uint64 packageSequence, uint8 channelId)
	handlePackageFunc = NewFuncWrapper(crossChainAbi, "handlePackage")
)

func StreamHandler(wr io.Writer, fmtr logger.Format, level logger.Lvl) logger.Handler {
	h := logger.FuncHandler(func(r *logger.Record) error {
		if r.Lvl > level {
			return nil
		}
		_, err := wr.Write(fmtr.Format(r))
		return err
	})
	return logger.LazyHandler(logger.SyncHandler(h))
}
func SimpleFormat() logger.Format {
	return logger.FormatFunc(func(r *logger.Record) []byte {
		buf := &bytes.Buffer{}
		fmt.Fprintf(buf, "%v|%v ", r.Time.Format("2006-01-02 15:04:05"), r.Lvl.AlignedString())
		ctxLen := len(r.Ctx)
		if ctxLen > 3 && r.Ctx[ctxLen-1] == "Normalized odd number of arguments by adding nil" {
			r.Ctx = r.Ctx[:ctxLen-3]
		}
		fmt.Fprintf(buf, r.Msg, r.Ctx...)
		buf.WriteByte('\n')
		return buf.Bytes()
	})
}
func initLogger() {
	logger.Root().SetHandler(StreamHandler(os.Stdout, SimpleFormat(), logger.LvlInfo))
}

type UInt64Slice []uint64

func (x UInt64Slice) Len() int           { return len(x) }
func (x UInt64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x UInt64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func fetchValidatorRewards(endpoint string, fromBlk uint64, toBlk uint64, fetchStep uint64, enableReward20 bool) {
	client, err := ethclient.Dial(endpoint)
	utils.PanicErr(err)

	// global context, save state
	ctx := NewRewardContext(enableReward20)

	// build event handler
	eventHandlers := []*EventHandler{
		NewEventHandler(validatorSharingEvent, nil),
		NewEventHandler(validatorDepositEvent, func(logs []*types.Log) error {
			_, err2 := ctx.CurTerm()
			if err2 != nil && err2 != ErrNotFoundTerm {
				return err2
			}

			// not found just pass
			if err2 == ErrNotFoundTerm {
				return nil
			}
			log0 := logs[0]

			vd, err2 := validatorDepositEvent.FilterAndParseLog(logs)
			if err2 != nil {
				return err2
			}
			val := vd[0][0].(common.Address).String()
			deposit := vd[0][1].(*big.Int)

			vs, err2 := validatorSharingEvent.FilterAndParseLog(logs)
			if err2 != nil && err2 != ErrNotFoundLog {
				return err2
			}
			sharing := big.NewInt(0)
			if len(vs) > 0 {
				sharing = vs[0][1].(*big.Int)
			}

			logger.Info("[Block Reward] block %v %v validator %v deposit %v sharing %v tx %v %v",
				log0.BlockNumber, log0.BlockHash, val, deposit, sharing, log0.TxIndex, log0.TxHash)
			err2 = ctx.RecordDeposit(log0.BlockNumber, val, deposit, sharing)
			if err2 != nil {
				return err2
			}

			return nil
		}),
		NewEventHandler(validatorMisdemeanorEvent, func(logs []*types.Log) error {
			vs, err2 := validatorMisdemeanorEvent.FilterAndParseLog(logs)
			if err2 != nil {
				return err2
			}
			log0 := logs[0]
			logger.Error("[ValidatorMisdemeanor] tx %v %v validator %v %v", log0.BlockNumber, log0.TxHash, vs[0][0], vs[0][1])

			err2 = ctx.RecordSlash(log0.BlockNumber, vs[0][0].(common.Address).String(), vs[0][1].(*big.Int), false)
			if err2 != nil {
				return err2
			}
			return nil
		}),
		NewEventHandler(validatorFelonyEvent, func(logs []*types.Log) error {
			vs, err2 := validatorFelonyEvent.FilterAndParseLog(logs)
			if err2 != nil {
				return err2
			}
			log0 := logs[0]
			logger.Error("[ValidatorFelony] tx %v %v validator %v %v", log0.BlockNumber, log0.TxHash, vs[0][0], vs[0][1])

			err2 = ctx.RecordSlash(log0.BlockNumber, vs[0][0].(common.Address).String(), vs[0][1].(*big.Int), true)
			if err2 != nil {
				return err2
			}
			return nil
		}),
		//event crossChainPackageEvent();
		NewEventHandler(crossChainPackageEvent, func(logs []*types.Log) error {
			log0 := logs[0]
			logger.Info("[Start crossChainPackage] tx %v %v", log0.BlockNumber, log0.TxHash)
			return nil
		}),
		//event validatorSetUpdated();
		NewEventHandler(validatorSetUpdatedEvent, func(logs []*types.Log) error {
			// recall fetch cross chain updateValidatorSet tx
			log0 := logs[0]
			logger.Info("[Start fetch updateValidatorSet] tx %v %v", log0.BlockNumber, log0.TxHash)
			blk, err2 := client.BlockByHash(context.Background(), log0.BlockHash)
			if err2 != nil {
				return err2
			}
			time.Sleep(DelayInParseEvent)
			receipt, err2 := client.TransactionReceipt(context.Background(), log0.TxHash)
			if err2 != nil {
				return err2
			}
			time.Sleep(DelayInParseEvent)

			term, err2 := ctx.CurTerm()
			if err2 != nil && err2 != ErrNotFoundTerm {
				return err2
			}

			if term != nil {
				// parse cross chain transferOut
				valMap := make(map[string]*big.Int, len(term.Validators))
				cp, err2 := crossChainPackageEvent.FilterAndParseLog(receipt.Logs)
				if err2 != nil && err2 != ErrNotFoundLog {
					return err2
				}
				if len(cp) > 0 {
					payload := cp[0][4].([]byte)
					pkg := LoadTransferOutPkgFromRLPBytes(payload[33:])
					for index, recipient := range pkg.Recipients {
						for _, v := range term.Validators {
							if v.BBCFeeAddress == recipient.String() {
								valMap[v.Address] = &pkg.Amounts[index]
								// recover BSC Precision
								valMap[v.Address].Mul(valMap[v.Address], Precision)
								break
							}
						}
					}
					if len(valMap) != len(pkg.Recipients) {
						return errors.New("transferOut not found some validators")
					}
				}

				// parse direct transfer
				dt, err2 := directTransferEvent.FilterAndParseLog(receipt.Logs)
				if err2 != nil && err2 != ErrNotFoundLog {
					return err2
				}

				if len(dt) > 0 {
					for _, item := range dt {
						valMap[item[0].(common.Address).String()] = item[1].(*big.Int)
					}
				}

				sysReward := new(big.Int).SetUint64(0)
				st, err2 := systemTransferEvent.FilterAndParseLog(receipt.Logs)
				if err2 != nil && err2 != ErrNotFoundLog {
					return err2
				}
				if len(st) > 0 {
					sysReward = st[0][0].(*big.Int)
				}

				err2 = ctx.TerminalTerm(client, blk, NewRewardInfo(valMap, sysReward))
				if err2 != nil {
					return err2
				}
			}

			// parse next validator
			tx, _, err2 := client.TransactionByHash(context.Background(), log0.TxHash)
			if err2 != nil {
				return err2
			}
			time.Sleep(DelayInParseEvent)
			args, err2 := handlePackageFunc.ParseArgs(tx.Data())
			if err2 != nil {
				return err2
			}
			payload2 := args[0].([]byte)
			setPkg := LoadValidatorSetPkgFromRLPBytes(payload2[33:])
			vals := make([]*Validator, 0, len(setPkg.ValidatorSet))
			logger.Info("[Validator set info]")
			for _, v := range setPkg.ValidatorSet {
				newVal := &Validator{
					Address:       v.Address.String(),
					FeeAddress:    v.FeeAddress.String(),
					BBCFeeAddress: v.BBCFeeAddress.String(),
					VotingPower:   v.VotingPower,
					Jailed:        false,
					Incoming:      new(big.Int).SetUint64(0),
					IsMaintaining: false,
					BlockCounter:  0,
				}
				logger.Info("[Validator info] %v %v %v %v", newVal.Address, newVal.FeeAddress, newVal.BBCFeeAddress, newVal.VotingPower)
				vals = append(vals, newVal)
			}

			err2 = ctx.NewTerm(NewValidatorTerm(blk, vals))
			if err2 != nil {
				return err2
			}
			return nil
		}),
	}

	logger.Debug("filter event topics:")
	handlerMap := make(map[string]*EventHandler, len(eventHandlers))
	for _, item := range eventHandlers {
		topic0 := item.event.ev.ID.String()
		logger.Debug("\t%v/%v: %v", item.event.contractName, item.event.ev.Name, topic0)
		handlerMap[topic0] = item
	}
	logger.Debug("====================================================================================================\n")

	topics := make([]common.Hash, 0, len(handlerMap))
	for k, _ := range handlerMap {
		topics = append(topics, common.HexToHash(k))
	}

	offset := fromBlk
	for {
		if offset >= toBlk {
			break
		}
		target := toBlk
		if toBlk-offset > fetchStep {
			target = offset + fetchStep
		}

		// fetch logs
		logger.Debug("[Start fetch] from %v to %v", offset, target)
		logs, err := client.FilterLogs(context.Background(), ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(offset),
			ToBlock:   new(big.Int).SetUint64(target),
			Addresses: []common.Address{
				// validatorSet
				common.HexToAddress("0x0000000000000000000000000000000000001000"),
				// crossChain
				common.HexToAddress("0x0000000000000000000000000000000000002000"),
			},
			Topics: [][]common.Hash{
				topics,
			},
		})
		utils.PanicErr(err)
		logger.Info("[Start fetch] from %v to %v logs %v", offset, target, len(logs))

		// handle logs
		logMapByBlock := make(map[uint64]map[uint64][]*types.Log)
		for i := range logs {
			number := logs[i].BlockNumber
			txIndex := uint64(logs[i].TxIndex)
			if _, ok := logMapByBlock[number]; !ok {
				logMapByBlock[number] = make(map[uint64][]*types.Log, 10)
			}
			logMapByBlock[number][txIndex] = append(logMapByBlock[number][txIndex], &logs[i])
		}

		// consumer by order
		orderBlk := make([]uint64, 0, len(logMapByBlock))
		for blk, _ := range logMapByBlock {
			orderBlk = append(orderBlk, blk)
		}

		sort.Sort(UInt64Slice(orderBlk))
		for _, blk := range orderBlk {
			logMapByTx := logMapByBlock[blk]
			printBlk := false
			orderTx := make([]uint64, 0, len(logMapByTx))
			for index, _ := range logMapByTx {
				orderTx = append(orderTx, index)
			}
			sort.Sort(UInt64Slice(orderTx))

			for _, index := range orderTx {
				items := logMapByTx[index]
				log0 := items[0]
				if !printBlk {
					logger.Debug("block number %v:%v", log0.BlockNumber, log0.BlockHash)
					printBlk = true
				}
				logger.Debug("\ttx %v:%v", log0.TxIndex, log0.TxHash)
				for _, log := range items {
					if e, ok := handlerMap[log.Topics[0].String()]; ok && e.handler != nil {
						if validatorSetUpdatedEvent == e.event {
							logger.Debug("[Find even handler] blk %v %v tx %v %v event %v %v",
								log0.BlockNumber, log0.BlockHash, log0.TxIndex, log0.TxHash, e.event.contractName, e.event.ev.Name)
						}
						if err2 := e.handler(items); err2 != nil && err2 != ErrNotFoundLog {
							logger.Error("\t\tevent %v:%v, blk: %v, txIndex: %v, decode err %v",
								e.event.contractName, e.event.ev.Name, blk, index, err2)
						}
						break
					}
				}
			}
			logger.Debug("====================================================================================================\n")
		}

		offset = target + 1
		time.Sleep(DelayInLogFilter)
	}
}

func init() {
	initLogger()
}

func main() {
	if len(os.Args) < 5 {
		panic("pls input endpoint fromBlock toBlock enableReward20 params")
	}

	endpoint := os.Args[1]
	fromBlk := utils.ParseInt64(os.Args[2])
	toBlk := utils.ParseInt64(os.Args[3])
	enableReward20, err := strconv.ParseBool(os.Args[4])
	utils.PanicErr(err)

	DelayInEmptyFetch = 1 * time.Millisecond
	DelayInLogFilter = 1 * time.Millisecond
	DelayInParseEvent = 1 * time.Millisecond
	fetchValidatorRewards(endpoint, uint64(fromBlk), uint64(toBlk), 3000, enableReward20)
}
