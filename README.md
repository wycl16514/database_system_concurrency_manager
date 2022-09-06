在上一节中我们实现了并发管理的核心组件那就是lock_table,它的原理是对给定的区块加锁，如果区块被读取，那么就加上共享锁，也就是多个线程能同时读取，但是不允许任何线程写入，如果有线程要写入，那么就必须加上互斥锁，此时其他线程都不能对给定区块进行读或写。

本节我们在lock_table的基础上实现并发管理器，它的实现原理比较简单，基本上都是直接调用lock_table,需要注意的是并发管理器可以有多个实例，但是他们都必须共同使用一个lock_table,添加文件concurrency_manager.go，添加代码如下：

```go
package tx

import (
	fm "file_manager"
)

type ConCurrencyManager struct {
	lock_table *LockTable
	lock_map   map[fm.BlockId]string
}

func NewConcurrencyManager() *ConCurrencyManager {
	concurrency_mgr := &ConCurrencyManager{
		lock_table: GetLockTableInstance(),
		lock_map:   make(map[fm.BlockId]string),
	}

	return concurrency_mgr
}

func (c *ConCurrencyManager) SLock(blk *fm.BlockId) error {
	_, ok := c.lock_map[*blk]
	if !ok {
		err := c.lock_table.SLock(blk)
		if err != nil {
			return err
		}
		c.lock_map[*blk] = "S"
	}
	return nil
}

func (c *ConCurrencyManager) XLock(blk *fm.BlockId) error {
	if !c.hasXLock(blk) {
		//c.SLock(blk) //判断区块是否已经被加上共享锁，如果别人已经获得共享锁那么就会挂起
		err := c.lock_table.XLock(blk)
		if err != nil {
			return err
		}
		c.lock_map[*blk] = "X"
	}

	return nil
}

func (c *ConCurrencyManager) Release() {
	for key, _ := range c.lock_map {
		c.lock_table.UnLock(&key)
	}
}

func (c *ConCurrencyManager) hasXLock(blk *fm.BlockId) bool {
	lock_type, ok := c.lock_map[*blk]
	return ok && lock_type == "X"
}

```
我们可以看到，并发管理器的实现很大层度上是对lock_table的调用，然后我们在tx.go的实现中启动并发管理器，由于这里就是几句代码因此具体内容请参看b站，搜索coding迪斯尼。这里需要注意的是，在map中我们使用对象作为key，而不是对象的指针作为key，原来使用指针作为key是错误，因为不同的指针指向的数据有可能是同一个区块，例如：

```go
blk1 := fm.NewBlockId("testfile", 1)
blk2 := fm.NewBlockId("testfile", 1)
```
blk1 和blk2 是两个数值不同的指针，他们的值不同，但是指向的却是同一个区块，因此如果用指针作为map的key就会出现问题。我们上一节实现的lock_table也有这个问题，所以我们修改后的代码也放在下面，同时修改了一些关于多线程的问题也在里面做了注释,修改后lock_table.go的内容如下：
```go
package tx

import (
	"errors"
	fm "file_manager"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	MAX_WAITING_TIME = 10 //3用于测试，在正式使用时设置为10
)

type LockTable struct {
	lock_map    map[fm.BlockId]int64           //将锁和区块对应起来
	notify_chan map[fm.BlockId]chan struct{}   //用于实现超时回退的管道
	notify_wg   map[fm.BlockId]*sync.WaitGroup //用于实现唤醒通知
	method_lock sync.Mutex                     //实现方法调用的线程安全，相当于java的synchronize关键字
}

var lock_table_instance *LockTable
var lock = &sync.Mutex{}

func GetLockTableInstance() *LockTable {
	lock.Lock()
	defer lock.Unlock()
	if lock_table_instance == nil {
		lock_table_instance = NewLockTable()
	}

	return lock_table_instance
}

func (l *LockTable) waitGivenTimeOut(blk *fm.BlockId) {
	wg, ok := l.notify_wg[*blk]
	if !ok {
		var new_wg sync.WaitGroup
		l.notify_wg[*blk] = &new_wg
		wg = &new_wg
	}
	wg.Add(1)
	defer wg.Done()
	l.method_lock.Unlock() //挂起前释放方法锁
	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		fmt.Println("routine wake up for timeout")
	case <-l.notify_chan[*blk]:
		fmt.Println("routine wake up by notify channel")
	}
	l.method_lock.Lock() //唤起后加上方法锁
}

func (l *LockTable) notifyAll(blk *fm.BlockId) {
	s := fmt.Sprintf("close channle for blk :%v\n", *blk)
	fmt.Println(s)

	channel, ok := l.notify_chan[*blk]
	if ok {
		close(channel)
		delete(l.notify_chan, *blk)
		mark := rand.Intn(10000)

		s := fmt.Sprintf("delete blk: %v and launch rotinue to create it, mark: %d\n", *blk, mark)
		fmt.Print(s)

		go func(blk_unlock fm.BlockId, ran_num int) {
			//等待所有线程返回后再重新设置channel
			//注意这个线程不一定得到及时调度，因此可能不能及时创建channel对象从而导致close closed channel panic
			s := fmt.Sprintf("wait group for blk: %v, with mark:%d\n", blk_unlock, ran_num)
			fmt.Print(s)
			l.notify_wg[blk_unlock].Wait()
			//访问内部数据时需要加锁
			l.method_lock.Lock()
			l.notify_chan[blk_unlock] = make(chan struct{})
			l.method_lock.Unlock()
			s = fmt.Sprintf("create notify channel for %v\n", blk_unlock)
			fmt.Print(s)

		}(*blk, mark)
	} else {
		s = fmt.Sprintf("channel for %v is already closed\n", *blk)
		fmt.Print(s)
	}
}

func NewLockTable() *LockTable {
	/*
		如果给定blk对应的值为-1，表明有互斥锁,如果大于0表明有相应数量的共享锁加在对应区块上，
		如果是0则表示没有锁
	*/
	lock_table := &LockTable{
		lock_map:    make(map[fm.BlockId]int64),
		notify_chan: make(map[fm.BlockId]chan struct{}),
		notify_wg:   make(map[fm.BlockId]*sync.WaitGroup),
	}

	return lock_table
}

func (l *LockTable) initWaitingOnBlk(blk *fm.BlockId) {
	_, ok := l.notify_chan[*blk]
	if !ok {
		l.notify_chan[*blk] = make(chan struct{})
	}

	_, ok = l.notify_wg[*blk]
	if !ok {
		l.notify_wg[*blk] = &sync.WaitGroup{}
	}
}

func (l *LockTable) SLock(blk *fm.BlockId) error {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()
	l.initWaitingOnBlk(blk)

	start := time.Now()
	for l.hasXlock(blk) && !l.waitingTooLong(start) {
		l.waitGivenTimeOut(blk)
	}
	//如果等待过长时间，有可能是产生了死锁
	if l.hasXlock(blk) {
		fmt.Println("slock fail for xlock")
		return errors.New("SLock Exception: XLock on given blk")
	}

	val := l.getLockVal(blk)
	l.lock_map[*blk] = val + 1
	return nil
}

func (l *LockTable) XLock(blk *fm.BlockId) error {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()
	l.initWaitingOnBlk(blk)

	start := time.Now()
	for l.hasOtherSLocks(blk) && !l.waitingTooLong(start) {
		fmt.Println("get xlock fail and sleep")
		l.waitGivenTimeOut(blk)
	}

	if l.hasOtherSLocks(blk) {
		return errors.New("XLock error: SLock on given blk")
	}

	//-1表示区块被加上互斥锁
	l.lock_map[*blk] = -1

	return nil
}

func (l *LockTable) UnLock(blk *fm.BlockId) {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()

	val := l.getLockVal(blk)
	if val > 1 {
		l.lock_map[*blk] = val - 1
	} else {
		delete(l.lock_map, *blk)
		//通知所有等待给定区块的线程从Wait中恢复
		s := fmt.Sprintf("unlock by blk: +%v\n", *blk)
		fmt.Println(s)
		l.notifyAll(blk)
	}
}

func (l *LockTable) hasXlock(blk *fm.BlockId) bool {
	return l.getLockVal(blk) < 0
}

func (l *LockTable) hasOtherSLocks(blk *fm.BlockId) bool {
	return l.getLockVal(blk) >= 1
}

func (l *LockTable) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_WAITING_TIME {
		return true
	}

	return false
}

func (l *LockTable) getLockVal(blk *fm.BlockId) int64 {
	val, ok := l.lock_map[*blk]
	if !ok {
		l.lock_map[*blk] = 0
		return 0
	}

	return val
}

```
接下来我们设计针对CurrencyManager的测试代码,增加一个文件名为concurrency_mgr_test.go,添加代码如下：
```
package tx

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"testing"
	"time"
)

func TestCurrencyManager(_ *testing.T) {
	file_manager, _ := fm.NewFileManager("txtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)
	//tx.NewTransation(file_manager, log_manager, buffer_manager)
	go func() {
		txA := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txA.Pin(blk1)
		txA.Pin(blk2)
		fmt.Println("Tx A: rquest slock 1")
		txA.GetInt(blk1, 0) //如果返回错误，我们应该放弃执行下面操作并执行回滚，这里为了测试而省略
		fmt.Println("Tx A: receive slock 1")
		time.Sleep(2 * time.Second)
		fmt.Println("Tx A: request slock 2")
		txA.GetInt(blk2, 0)
		fmt.Println("Tx A: receive slock 2")
		fmt.Println("Tx A: Commit")
		txA.Commit()
	}()

	go func() {
		time.Sleep(1 * time.Second)
		txB := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txB.Pin(blk1)
		txB.Pin(blk2)
		fmt.Println("Tx B: rquest xlock 2")
		txB.SetInt(blk2, 0, 0, false)
		fmt.Println("Tx B: receive xlock 2")
		time.Sleep(2 * time.Second)
		fmt.Println("Tx B: request slock 1")
		txB.GetInt(blk1, 0)
		fmt.Println("Tx B: receive slock 1")
		fmt.Println("Tx B: Commit")
		txB.Commit()
	}()

	go func() {
		time.Sleep(2 * time.Second)
		txC := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txC.Pin(blk1)
		txC.Pin(blk2)
		fmt.Println("Tx C: rquest xlock 1")
		txC.SetInt(blk1, 0, 0, false)
		fmt.Println("Tx C: receive xlock 1")
		time.Sleep(1 * time.Second)
		fmt.Println("Tx C: request slock 2")
		txC.GetInt(blk2, 0)
		fmt.Println("Tx C: receive slock 2")
		fmt.Println("Tx C: Commit")
		txC.Commit()
	}()

	time.Sleep(20 * time.Second)
}

```

上面测试用例的基本逻辑为，我们创建三个线程，每个线程对应一个交易，每个交易都写入或读取共同的区块。我们特意让交易的执行次序为A,B,C，也就是A交易先执行，然后是B，最后是C，在交易A执行后会暂停一会，让交易B,C得到执行的机会，上面用例执行后输出情况如下：
```
Tx A: rquest slock 1
Tx A: receive slock 1
Tx B: rquest xlock 2
Tx B: receive xlock 2
Tx A: request slock 2
Tx C: rquest xlock 1
get xlock fail and sleep
Tx B: request slock 1
Tx B: receive slock 1
Tx B: Commit
unlock by blk: +{testfile 2}

close channle for blk :{testfile 2}

delete blk: {testfile 2} and launch rotinue to create it, mark: 8081
routine wake up by notify channel
Tx A: receive slock 2
Tx A: Commit
unlock by blk: +{testfile 2}

close channle for blk :{testfile 2}

channel for {testfile 2} is already closed
unlock by blk: +{testfile 1}

close channle for blk :{testfile 1}

delete blk: {testfile 1} and launch rotinue to create it, mark: 7887
wait group for blk: {testfile 2}, with mark:8081
create notify channel for {testfile 2}
routine wake up by notify channel
Tx C: receive xlock 1
wait group for blk: {testfile 1}, with mark:7887
create notify channel for {testfile 1}
transation 2  committed
transation 1  committed
Tx C: request slock 2
Tx C: receive slock 2
Tx C: Commit
unlock by blk: +{testfile 1}

close channle for blk :{testfile 1}

delete blk: {testfile 1} and launch rotinue to create it, mark: 1847
unlock by blk: +{testfile 2}

close channle for blk :{testfile 2}

delete blk: {testfile 2} and launch rotinue to create it, mark: 4059
wait group for blk: {testfile 1}, with mark:1847
create notify channel for {testfile 1}
wait group for blk: {testfile 2}, with mark:4059
create notify channel for {testfile 2}
transation 3  committed
```
我们需要针对以上输出进行一些分析。首先交易A启动时在区块1(blk1)上获取共享锁，然后交易B启动时针对区块2(blk2)获取互斥锁,这里对应以下四句输出：
```
Tx A: rquest slock 1
Tx A: receive slock 1
Tx B: rquest xlock 2
Tx B: receive xlock 2
Tx A: request slock 2
```
接下来交易C想获取区块1的互斥锁，交易A想获得区块2的共享锁，由于这两个区块已经被加锁，所以他们在获取时会被挂起，这里对应输出为：
```
Tx A: request slock 2
Tx C: rquest xlock 1
get xlock fail and sleep
```
接着交易B继续获取区块1的共享锁，由于交易A在区块1上加的是共享锁因此交易B同样也能获得共享锁，这里对应输出为：
```
Tx B: request slock 1
Tx B: receive slock 1
```
当交易B完成后它执行Commit，同时释放所有加在区块上的锁，这里对应输出如下：
```
unlock by blk: +{testfile 2}
close channle for blk :{testfile 2}
delete blk: {testfile 2} and launch rotinue to create it, mark: 8081
```
上面输出是交易B释放对区块2的锁，同时关闭针对区块2的管道，这样就能通知所有等待在区块2上的交易，同时创建一个标号为8081的线程去探测是否所有等待区块2的线程都已经恢复，是的话就为区块2重新创建管道对象，这里交易B还会释放区块1上的锁，但由于区块1上面有两把共享锁，因此交易B的Commit执行后会把区块1上的共享锁数量减1，由于交易A正在等待区块2，因此上面对区块2的释放就会激活交易A所在线程，相关输出如下：
```
routine wake up by notify channel  
Tx A: receive slock 2
Tx A: Commit
```
"routine wake up by notify channel  "这句输出表示交易A所在线程被唤醒，然后交易A获取区块2，然后交易完成，执行Commit操作并释放它加在区块1和2上的锁，它首先释放的也是加在区块2上的锁：
```
unlock by blk: +{testfile 2}

close channle for blk :{testfile 2}

channel for {testfile 2} is already closed
```
这里要注意的是，在交易C完成时也释放了区块2对应的管道，但是当时用于再次创建区块2管道的线程，也就是标号为8081的线程还没有执行的机会，因此这里再次去关闭区块2对应管道时能检测到管道已经处于关闭状态。接下来是区块1对应的管道被关闭，用于通知那些等待区块1的线程：
```
unlock by blk: +{testfile 1}

close channle for blk :{testfile 1}

delete blk: {testfile 1} and launch rotinue to create it, mark: 7887
```
到这里，加在区块1上的共享锁全部被解除，因此关闭区块1上的管道，唤醒那些要获取区块1互斥锁的线程，也就是交易C所在线程，同时发起一个线程去为区块1重新设置管道。接下来就是交易C所在的线程开始执行：
```
wait group for blk: {testfile 2}, with mark:8081
create notify channel for {testfile 2}
routine wake up by notify channel
Tx C: receive xlock 1
wait group for blk: {testfile 1}, with mark:7887
create notify channel for {testfile 1}
transation 2  committed
transation 1  committed
Tx C: request slock 2
Tx C: receive slock 2
Tx C: Commit
```
从上面输出可以看到，直到这里在交易C完成时创建的标号为8081的线程才得到了调度的机会，因此它为区块2重新创建了管道对象。同时交易C对应线程被唤醒，然后他如愿以偿获得了区块1的互斥锁，然后继续获得区块2的共享锁，然后执行Commit操作，这里交易C会释放加在区块1和区块2上的锁：
```
unlock by blk: +{testfile 1}

close channle for blk :{testfile 1}

delete blk: {testfile 1} and launch rotinue to create it, mark: 1847
unlock by blk: +{testfile 2}

close channle for blk :{testfile 2}

delete blk: {testfile 2} and launch rotinue to create it, mark: 4059
wait group for blk: {testfile 1}, with mark:1847
create notify channel for {testfile 1}
wait group for blk: {testfile 2}, with mark:4059
create notify channel for {testfile 2}
transation 3  committed
```
在区块的锁释放后，lock_table会再次启动两个线程去创建这两个区块对应的管道对象，用于创建区块1的线程标号为1847，用于创建区块2的线程标号为4059，然后这两个线程分别执行并为对应区块创建管道对象，此时所有交易完成。我们从上面输出的信息可以看到，并发管理器能有效的针对不同线程中的交易在读写对应区块时准确加锁，保证区块的读写顺序满足可序列化原则，进而确保在多并发情况下，每个交易都能正确执行且不会互相影响，更详细的调试演示请在B站搜索Coding迪斯尼，

