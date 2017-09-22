package mq

import (
	"sync"
)

const (
	// default limit
	MAX_POOL_NUM     = 200
	MIN_POOL_NUM     = 80
	DEFAULT_POOL_NUM = 80
)

type PoolManager struct {
	connPool       chan *RabbitmqConfig
	lock           sync.Mutex
	connTmp        RabbitmqConfig
	maxPoolNum     int
	minPoolNum     int
	defaultPoolNum int
	CurrentCounter int
}

func NewConnPool(rc RabbitmqConfig, nums ...int) PoolManager {
	if len(nums) != 3 && len(nums) > 1 {
		panic("error")
	}

	switch len(nums) {
	case 3:
		conns := make(chan *RabbitmqConfig, nums[0])
		pool := PoolManager{
			connPool:       conns,
			connTmp:        rc,
			maxPoolNum:     nums[0],
			minPoolNum:     nums[1],
			defaultPoolNum: nums[2],
		}
		for i := 0; i < nums[2]; i++ {
			rc_t := rc
			pool.connPool <- &rc_t
			// pool.connPool <- &rc
		}
		pool.CurrentCounter = pool.defaultPoolNum
		return pool

	default:
		conns := make(chan *RabbitmqConfig, MAX_POOL_NUM)
		pool := PoolManager{
			connPool:       conns,
			connTmp:        rc,
			maxPoolNum:     MAX_POOL_NUM,
			minPoolNum:     MIN_POOL_NUM,
			defaultPoolNum: DEFAULT_POOL_NUM,
		}

		for i := 0; i < DEFAULT_POOL_NUM; i++ {
			rc_t := rc
			pool.connPool <- &rc_t
		}
		pool.CurrentCounter = pool.defaultPoolNum

		return pool
	}

}

func (p *PoolManager) SetConnPoolConfig(max int, min int, default_num int) {
}

func (p *PoolManager) GetPoolConnBlock() (*RabbitmqConfig, error) {
	v := <-p.connPool
	return v, nil
}

func (p *PoolManager) GetPoolConn() (*RabbitmqConfig, error) {
	conn, ok := <-p.connPool
	if ok {
		return conn, nil
	}

	if p.CurrentCounter > p.maxPoolNum {
		// will block
		conn := <-p.connPool
		return conn, nil
	}
	p.CurrentCounter += 1
	// fix
	temp_conn := new(RabbitmqConfig)
	*temp_conn = p.connTmp

	return temp_conn, nil
}

// 是否考虑开一个go来做阻塞的insert
func (p *PoolManager) WhileFeedsConn() {
}

func (p *PoolManager) AddNewConn() {
	temp := new(RabbitmqConfig)
	*temp = p.connTmp
	p.connPool <- temp
}

func (p *PoolManager) ReleaseConn(conn *RabbitmqConfig) {
	p.connPool <- conn
}

func (p *PoolManager) CheckAvailConn() {

}

func (p *PoolManager) QueryPoolLength() {

}

func (p *PoolManager) TryPingPong() {

}

