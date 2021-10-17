//#include "MyQueue.h"

//#include <queue>

#include <pthread.h>

template <class T>
MyQueue<T>::MyQueue()
{
//	Queue = new std::queue<T>(); 
	pthread_rwlock_init(&rwlock, NULL);
}

//template class MyQueue<int>;