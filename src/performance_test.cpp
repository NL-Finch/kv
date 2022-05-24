/*------------------------------------------------------------------\
|-> File Name:     performance_test.cpp
|-> Author:        NL-KB
|-> Created Time:  2022.5.16
|-> Description:   对KV引擎的性能测试
\------------------------------------------------------------------*/
#include <thread>
#include <chrono>
#include <vector>
#include "kvengine.hpp"

#define THREADS_NUM 3
#define INSERT_NUM 300000
#define KEY_MAX 1000000

KV::KVEngine<int, std::string> kv(20);

void insert_test(int tid)
{
	srand(tid);
	for (int i = 0; i < (INSERT_NUM / THREADS_NUM); i++)
	{
		kv.insert(rand() % KEY_MAX, "abcdefg");
	}

}

void find_test(int tid)
{
	srand(tid);
	for (int i = 0; i < (INSERT_NUM / THREADS_NUM); i++)
	{
		kv.find(rand() % KEY_MAX);
	}

}

auto run_test(void (*test_func)(int tid))
{
	std::vector<std::thread> threadList;
	auto start = std::chrono::system_clock::now();
	for (int idx = 0; idx < THREADS_NUM; ++idx)
	{
		threadList.emplace_back(std::thread{ test_func, idx });  // emplace_back防止对象复制构造
	}
	for (int idx = 0; idx < THREADS_NUM; ++idx)
	{
		threadList[idx].join();
	}
	auto end = std::chrono::system_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
	return  duration.count();
}

int main()
{

	// 插入测试
	auto insert_duration = run_test(insert_test);

	// 查找测试
	auto find_duration = run_test(find_test);

	std::cout << " insert using: " << insert_duration << " milliseconds" << std::endl;
	std::cout << " find using:   " << find_duration << " milliseconds" << std::endl;
}