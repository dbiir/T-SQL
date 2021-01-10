#include "postgres.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <assert.h>
#include "port.h"
#include "storage/threadpool.h"
#include "tdb/storage_processor.h"
#include "tdb/storage_param.h"
#include "utils/memutils.h"
#include <sys/prctl.h>

//share resource
static CThread_pool *pool = NULL;

static void
init_thread_param(void)
{
	prctl(PR_SET_NAME, "Thread pool");
	if (CurrentThreadMemoryContext == NULL)
		CurrentThreadMemoryContext =
			AllocSetContextCreate(TopMemoryContext,
								  "Storage Work Thread Top Context",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
	if (enable_thread_lock)
	{
		StorageHaveLock.HAVE_MemoryContext_LOCK = false;

		StorageHaveLock.HAVE_VisionJudge_LOCK = false;
		StorageHaveLock.HAVE_WBHTAB_LOCK = false;
		StorageHaveLock.HAVE_LWinte_LOCK = false;
		StorageHaveLock.HAVE_LWarray_LOCK = false;
		StorageHaveLock.HAVE_Update_LOCK = false;
		StorageHaveLock.HAVE_TupleLockHash_LOCK = false;
		for (int i = 0; i < TXN_BUCKET; i++)
		{
			StorageHaveLock.HAVE_TxnHTAB_LOCK[i] = false;
		}
	}

	if (enable_dynamic_lock)
		InitThreadHaveLock();
}
#if 0
static void
destroy_thread_param()
{
    destroyThreadHaveLock();
}
#endif
void pool_init(int max_thread_num)
{
	pool = (CThread_pool *)malloc(sizeof(CThread_pool));

	pthread_mutex_init(&(pool->queue_lock), NULL);
	pthread_cond_init(&(pool->queue_ready), NULL);

	pool->queue_head = NULL;

	pool->max_thread_num = max_thread_num;
	pool->cur_queue_size = 0;

	pool->shutdown = 0;

	pool->threadid = (pthread_t *)malloc(max_thread_num * sizeof(pthread_t));
	int i = 0;
	for (i = 0; i < max_thread_num; i++)
	{
		pthread_create(&(pool->threadid[i]), NULL, thread_routine, NULL);
	}
}

/*Add a task to the thread pool*/
int pool_add_worker(void *(*process)(void *arg), void *arg)
{
	/*Construct a new task*/
	CThread_worker *newworker = (CThread_worker *)malloc(sizeof(CThread_worker));
	newworker->process = process;
	newworker->arg = arg;
	newworker->next = NULL; /*Don't forget to leave blank*/

	pthread_mutex_lock(&(pool->queue_lock));
	/*Add tasks to the wait queue*/
	CThread_worker *member = pool->queue_head;
	if (member != NULL)
	{
		while (member->next != NULL)
			member = member->next;
		member->next = newworker;
	}
	else
	{
		pool->queue_head = newworker;
	}

	assert(pool->queue_head != NULL);

	pool->cur_queue_size++;
	pthread_mutex_unlock(&(pool->queue_lock));
	/*Ok, wait for the task in the queue, wake up a waiting thread;
     Note that this sentence has no effect if all threads are busy.*/
	pthread_cond_signal(&(pool->queue_ready));
	return 0;
}

/*Destroy the thread pool, waiting for the tasks in the queue will not be executed, but the running thread will always
Exit the task after it has finished running.*/
int pool_destroy()
{
	if (pool->shutdown)
		return -1; /*Prevent two calls*/
	pool->shutdown = 1;

	/*Wake up all waiting threads, the thread pool is destroyed*/
	pthread_cond_broadcast(&(pool->queue_ready));

	/*Blocking waiting for thread to exit*/
	int i;
	for (i = 0; i < pool->max_thread_num; i++)
		pthread_join(pool->threadid[i], NULL);
	free(pool->threadid);

	/*Destroy wait queue*/
	CThread_worker *head = NULL;
	while (pool->queue_head != NULL)
	{
		head = pool->queue_head;
		pool->queue_head = pool->queue_head->next;
		free(head);
	}
	/*Don't forget to destroy condition variables and mutexes*/
	pthread_mutex_destroy(&(pool->queue_lock));
	pthread_cond_destroy(&(pool->queue_ready));

	free(pool);
	/*It is a good habit to empty the pointer after destroying.*/
	pool = NULL;
	return 0;
}

void *
thread_routine(void *arg)
{
	init_thread_param();

	while (1)
	{
		pthread_mutex_lock(&(pool->queue_lock));

		while (pool->cur_queue_size == 0 && !pool->shutdown)
		{
			//printf ("thread 0x%x is waiting\n", (unsigned int)pthread_self ());
			pthread_cond_wait(&(pool->queue_ready), &(pool->queue_lock));
		}

		/*The thread pool is going to be destroyed.*/
		if (pool->shutdown)
		{
			/*In case of break, continue, return and other jump statements, don't forget to unlock first*/
			pthread_mutex_unlock(&(pool->queue_lock));
			//printf ("thread 0x%x will exit\n", (unsigned int)pthread_self ());
			pthread_exit(NULL);
		}

		//printf ("thread 0x%x is starting to work\n", (unsigned int)pthread_self ());

		assert(pool->cur_queue_size != 0);
		assert(pool->queue_head != NULL);

		/*Wait for the queue length minus 1 and remove the header element from the linked list*/
		pool->cur_queue_size--;
		CThread_worker *worker = pool->queue_head;
		pool->queue_head = worker->next;
		pthread_mutex_unlock(&(pool->queue_lock));

		/*Call the callback function to perform the task*/
		(*(worker->process))(worker->arg);
		free(worker);
		worker = NULL;
	}
	if (CurrentThreadMemoryContext != NULL)
	{
		MemoryContextDelete(CurrentThreadMemoryContext);
		CurrentThreadMemoryContext = NULL;
	}
	/*This sentence should be unreachable*/
	//destroy_thread_param();
	pthread_exit(NULL);
}
