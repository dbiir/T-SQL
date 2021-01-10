#include "postgres.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <assert.h>
#include "port.h"

/*
 * All running and waiting tasks in the thread pool are a CThread_worker
 * Because all tasks are in the linked list, it is a linked list structure
 */
typedef struct worker
{
    /* 
     * Callback function, this function will be called when the task is running,
     * and can be declared as other forms
     */
    void *(*process) (void *arg);
    void *arg;/*Callback function parameters*/
    struct worker *next;

} CThread_worker;

/*Thread pool structure*/
typedef struct
{
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_ready;

    /*Linked list structure, all waiting tasks in the thread pool*/
    CThread_worker *queue_head;

    /*Whether to destroy the thread pool*/
    int shutdown;
    pthread_t *threadid;//Thread array
    /*Number of active threads allowed in the thread pool*/
    int max_thread_num;
    /*Number of tasks currently waiting for the queue*/
    int cur_queue_size;

} CThread_pool;

extern void pool_init (int max_thread_num);
extern int pool_destroy (void);
extern int pool_add_worker (void *(*process) (void *arg), void *arg);
extern void *thread_routine (void *arg);
