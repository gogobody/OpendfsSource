#include <unistd.h>
#include <sys/eventfd.h>

#include "faio_notifier_manager.h"

static int faio_notifier_create(void);
static void faio_notifier_destroy(int *nfd);
static faio_atomic_t atomic_inc(faio_atomic_t *x);
static faio_atomic_t atomic_dec(faio_atomic_t *x);
static int atomic_test_and_set(faio_atomic_t *x);
static void atomic_test_and_reset(faio_atomic_t *x);
static inline unsigned char cmp_and_swap(volatile uint64_t *ptr, 
	uint64_t old, uint64_t nw);

// 初始化 notifier
// 创建 event fd 初始化 0
int faio_notifier_manager_init(faio_notifier_manager_t *notifier,
    faio_manager_t *fm, faio_errno_t *err_no)
{
    if (!notifier || !fm) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_INIT_PARAM;
		
        return FAIO_ERROR;
    }

    notifier->release = FAIO_FALSE;
    faio_notifier_count_reset(&notifier->count);// set count -> 0
    faio_notifier_count_reset(&notifier->noticed); // set noticed -> 0
    // 创建 event fd 初始化 0
    notifier->nfd = faio_notifier_create();
    if (notifier->nfd < 0) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_INIT_NFD_CREATE;
		
        return FAIO_ERROR;
    }
    // 初始化 锁和条件变量
    if (faio_condition_init(&notifier->cond) == FAIO_ERROR) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_INIT_CONDITION_INIT;
		
        return FAIO_ERROR;
    }
    
    notifier->manager = fm;
    
    return FAIO_OK;
}

int faio_notifier_manager_release(faio_notifier_manager_t *notifier, 
    faio_errno_t *err_no)
{
    if (!notifier) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_RELEASE_PARAM;
		
        return FAIO_ERROR;
    }

    if (notifier->release == FAIO_TRUE) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_RELEASE_TRUE;
		
        return FAIO_ERROR;
    }
	
    notifier->release = FAIO_TRUE;

    while (!faio_notifier_count_equal(&notifier->count, 0)) 
	{
        faio_condition_wait(&notifier->cond, FAIO_NOTIFIER_DEFAULT_TIMEOUT);
    }

    faio_notifier_destroy(&notifier->nfd);
    faio_condition_destroy(&notifier->cond);

    if (notifier->manager) 
	{
        notifier->manager = nullptr;
    }

    return FAIO_OK;
}

// eventfd 用于进程间通信
//eventfd是Linux 2.6提供的一种系统调用，它可以用来实现事件通知
static int faio_notifier_create(void)
{
    int nfd = 0;
    //创建eventfd时会返回一个文件描述符，进程可以通过对这个文件描述符进行read/write来读取/改变计数器的值，
    // 从而实现进程间通信。
    nfd = eventfd(0, 0);

    return nfd;
}

static void faio_notifier_destroy(int *nfd)
{
    if (*nfd >= 0) 
	{
        close(*nfd);
        *nfd = -1;
    }

    return;
}

int faio_notifier_send(faio_notifier_manager_t *notifier, 
	faio_errno_t *err_no)
{
    uint64_t count = 1;

    if (notifier->nfd < 0) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_SEND_PARAM;
		
        return FAIO_ERROR;
    }

    if (atomic_test_and_set(&notifier->noticed)) // set noticed -> 1
	{
        if (write(notifier->nfd, &count, sizeof(count)) < 0) 
		{
            err_no->sys = errno;
            err_no->notifier = FAIO_ERR_NOTIFIER_SEND_WRITE;
			
            return FAIO_ERROR;
        }
    }
    
    return FAIO_OK;
}

// 读取 eventfd
// 设置 noticed FAIO_FALSE ??
int faio_notifier_receive(faio_notifier_manager_t *notifier,
	faio_errno_t *err_no)
{
    uint64_t count = -1;

    if (notifier->nfd < 0) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_RECEIVE_PARAM;
		
        return FAIO_ERROR;
    }
    
    if (read(notifier->nfd, &count, sizeof(count)) < 0) 
	{
        err_no->sys = errno;
        err_no->notifier = FAIO_ERR_NOTIFIER_RECEIVE_READ;
		
        return FAIO_ERROR;
    }

    // reset noticed -> 0
    atomic_test_and_reset(&notifier->noticed);
    
    return FAIO_OK;
}

int faio_notifier_count_inc(faio_notifier_manager_t *notifier, 
    faio_errno_t *err_no)
{
    if (!notifier) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_COUNT_INC_PARAM;
		
        return FAIO_ERROR;
    }
    
    atomic_inc(&notifier->count);

    return FAIO_OK;
}

int faio_notifier_count_dec(faio_notifier_manager_t *notifier, 
    faio_errno_t *err_no)
{
    faio_atomic_t ref_count;
    
    if (!notifier) 
	{
        err_no->notifier = FAIO_ERR_NOTIFIER_COUNT_DEC_PARAM;
		
        return FAIO_ERROR;
    }
    
    ref_count = atomic_dec(&notifier->count);

    if (notifier->release == FAIO_TRUE
        && faio_notifier_count_equal(&ref_count, 0)) 
    {
        faio_condition_signal(&notifier->cond);
    }

    return FAIO_OK;
}

faio_atomic_t atomic_inc(faio_atomic_t *x)
{
    uint64_t      value;
    faio_atomic_t tmp;
    
    do 
	{
        value = x->value;
    } while (!CAS(&x->value, value, x->value + 1));
	
    tmp.value = value + 1;
    
    return tmp; 
}

faio_atomic_t atomic_dec(faio_atomic_t *x)
{
    uint64_t      value;
    faio_atomic_t tmp;
    
    do {
        value = x->value;
    } while (!CAS(&x->value, value, x->value - 1));
	
    tmp.value = value - 1;
    
    return tmp;
}

/*
 * test 0 and set 1
 */
static int atomic_test_and_set(faio_atomic_t *x)
{
    uint64_t value = FAIO_FALSE;
    uint64_t set = FAIO_TRUE;

    return CAS(&x->value, value, set);
}

/*
 * test 1 and set 0
 * CAS是compare and swap,   简单来说就是，在写入新值之前， 读出旧值， 当且仅当旧值与存储中的当前值一致时，才把新值写入存储
 */
static void atomic_test_and_reset(faio_atomic_t *x)
{
    uint64_t value;
    uint64_t set = FAIO_FALSE;
//    do{
//
//        备份旧数据；
//
//        基于旧数据构造新数据；
//
//    }while(!CAS( 内存的最新值，备份的旧数据，新数据 ))
    do {
        value = x->value;
    } while (!CAS(&x->value, value, set));

    return;
}

static inline unsigned char cmp_and_swap(volatile uint64_t *ptr, 
	uint64_t old, uint64_t nw)
{
    unsigned char ret;
    
    __asm__ __volatile__("lock; cmpxchgq %1,%2; setz %0"
            : "=a"(ret)
            : "q"(nw), "m"(*(volatile uint64_t *)(ptr)), "0"(old)
            : "memory");
    
    return ret;
}

